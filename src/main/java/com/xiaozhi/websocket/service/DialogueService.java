package com.xiaozhi.websocket.service;

import com.xiaozhi.entity.SysConfig;
import com.xiaozhi.entity.SysDevice;
import com.xiaozhi.websocket.llm.LlmManager;
import com.xiaozhi.websocket.service.VadService.VadStatus;
import com.xiaozhi.websocket.stt.SttService;
import com.xiaozhi.websocket.stt.factory.SttServiceFactory;
import com.xiaozhi.websocket.tts.factory.TtsServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * 对话处理服务
 * 负责处理语音识别和对话生成的业务逻辑
 */
@Service
public class DialogueService {
    private static final Logger logger = LoggerFactory.getLogger(DialogueService.class);

    @Autowired
    private LlmManager llmManager;

    @Autowired
    private AudioService audioService;

    @Autowired
    private TtsServiceFactory ttsService;

    @Autowired
    private SttServiceFactory sttServiceFactory;

    @Autowired
    private MessageService messageService;

    @Autowired
    private VadService vadService;

    @Autowired
    private SessionManager sessionManager;

    /**
     * 处理音频数据
     * 
     * @param session  WebSocket会话
     * @param opusData Opus格式的音频数据
     * @return 处理结果
     */
    public Mono<Void> processAudioData(WebSocketSession session, byte[] opusData) {
        String sessionId = session.getId();
        SysDevice device = sessionManager.getDeviceConfig(sessionId);

        // 如果设备未注册或不在监听状态，忽略音频数据
        if (device == null || !sessionManager.isListening(sessionId)) {
            return Mono.empty();
        }

        SysConfig sttConfig = null;
        SysConfig ttsConfig = null;

        if (device.getSttId() != null) {
            sttConfig = sessionManager.getCachedConfig(device.getSttId());
        }

        if (device.getTtsId() != null) {
            ttsConfig = sessionManager.getCachedConfig(device.getTtsId());
        }

        // 创建最终变量以在lambda中使用
        final SysConfig finalSttConfig = sttConfig;
        final SysConfig finalTtsConfig = ttsConfig;

        return Mono.fromCallable(() -> {
            // 使用VAD处理音频数据
            return vadService.processAudio(sessionId, opusData);
        })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(vadResult -> {
                    // 如果VAD处理出错，直接返回
                    if (vadResult.getStatus() == VadStatus.ERROR || vadResult.getProcessedData() == null) {
                        return Mono.empty();
                    }

                    // 根据VAD状态处理
                    switch (vadResult.getStatus()) {
                        case SPEECH_START:
                            // 检测到语音开始，初始化流式识别
                            return initializeStreamingRecognition(session, sessionId, finalSttConfig, finalTtsConfig,
                                    device,
                                    vadResult.getProcessedData());

                        case SPEECH_CONTINUE:
                            // 语音继续，发送数据到流式识别
                            if (sessionManager.isStreaming(sessionId)) {
                                Sinks.Many<byte[]> audioSink = sessionManager.getAudioSink(sessionId);
                                if (audioSink != null) {
                                    audioSink.tryEmitNext(vadResult.getProcessedData());
                                }
                            }
                            return Mono.empty();

                        case SPEECH_END:
                            // 语音结束，完成流式识别
                            if (sessionManager.isStreaming(sessionId)) {
                                Sinks.Many<byte[]> audioSink = sessionManager.getAudioSink(sessionId);
                                if (audioSink != null) {
                                    audioSink.tryEmitComplete();
                                    sessionManager.setStreamingState(sessionId, false);
                                }
                            }
                            return Mono.empty();

                        default:
                            return Mono.empty();
                    }
                })
                .onErrorResume(e -> {
                    logger.error("处理音频数据失败: {}", e.getMessage(), e);
                    return Mono.empty();
                });
    }

    /**
     * 初始化流式语音识别
     */
    private Mono<Void> initializeStreamingRecognition(
            WebSocketSession session,
            String sessionId,
            SysConfig sttConfig,
            SysConfig ttsConfig,
            SysDevice device,
            byte[] initialAudio) {

        // 如果已经在进行流式识别，先清理旧的资源
        sessionManager.closeAudioSink(sessionId);

        // 创建新的音频数据接收器
        Sinks.Many<byte[]> audioSink = sessionManager.createAudioSink(sessionId);
        sessionManager.setStreamingState(sessionId, true);

        // 获取对应的STT服务
        SttService sttService = sttServiceFactory.getSttService(sttConfig);

        if (sttService == null) {
            logger.error("无法获取STT服务 - Provider: {}", sttConfig != null ? sttConfig.getProvider() : "null");
            return Mono.empty();
        }

        // 发送初始音频数据
        if (initialAudio != null && initialAudio.length > 0) {
            audioSink.tryEmitNext(initialAudio);
        }

        // 创建最终变量以在lambda中使用
        final SysConfig finalTtsConfig = ttsConfig;

        // 启动流式识别
        sttService.streamRecognition(audioSink.asFlux())
                .doOnNext(text -> {
                    // 发送中间识别结果
                    if (StringUtils.hasText(text)) {
                        messageService.sendMessage(session, "stt", "interim", text).subscribe();
                    }
                })
                .last() // 获取最终结果
                .flatMap(finalText -> {
                    if (!StringUtils.hasText(finalText)) {
                        return Mono.empty();
                    }

                    // 设置会话为非监听状态，防止处理自己的声音
                    sessionManager.setListeningState(sessionId, false);

                    // 发送最终识别结果
                    return messageService.sendMessage(session, "stt", "final", finalText)
                            .then(Mono.fromRunnable(() -> {
                                // 使用句子切分处理流式响应
                                llmManager.chatStreamBySentence(device, finalText,
                                        (sentence, isStart, isEnd) -> {
                                            // 使用非流式TTS处理
                                            Mono.fromCallable(
                                                    () -> ttsService
                                                            .getTtsService(finalTtsConfig, device.getVoiceName())
                                                            .textToSpeech(
                                                                    sentence))
                                                    .subscribeOn(Schedulers.boundedElastic())
                                                    .flatMap(audioPath -> audioService
                                                            .sendAudioMessage(session, audioPath, sentence, isStart,
                                                                    isEnd)
                                                            .doOnError(e -> logger.error("发送音频消息失败: {}", e.getMessage(),
                                                                    e)))
                                                    .onErrorResume(e -> {
                                                        logger.error("处理句子失败: {}", e.getMessage(), e);
                                                        return Mono.empty();
                                                    })
                                                    .subscribe();
                                        });
                            }));
                })
                .onErrorResume(error -> {
                    logger.error("流式识别错误: {}", error.getMessage(), error);
                    return Mono.empty();
                })
                .subscribe();

        return Mono.empty();
    }

    /**
     * 处理语音唤醒
     * 
     * @param session WebSocket会话
     * @param text    唤醒词文本
     * @return 处理结果
     */
    public Mono<Void> handleWakeWord(WebSocketSession session, String text) {
        String sessionId = session.getId();
        SysDevice device = sessionManager.getDeviceConfig(sessionId);

        if (device == null) {
            return Mono.empty();
        }

        // 获取配置并创建final变量以在lambda中使用
        final SysConfig ttsConfig = device.getTtsId() != null ? sessionManager.getCachedConfig(device.getTtsId())
                : null;

        logger.info("检测到唤醒词: {}", text);

        // 设置为非监听状态，防止处理自己的声音
        sessionManager.setListeningState(sessionId, false);

        // 发送识别结果
        messageService.sendMessage(session, "stt", "start", text).subscribe();

        // 使用句子切分处理流式响应
        return Mono.fromRunnable(() -> {
            // 使用句子切分处理流式响应
            llmManager.chatStreamBySentence(device, text,
                    (sentence, isStart, isEnd) -> {
                        // 使用非流式TTS处理
                        Mono.fromCallable(
                                () -> ttsService.getTtsService(ttsConfig, device.getVoiceName()).textToSpeech(
                                        sentence))
                                .subscribeOn(Schedulers.boundedElastic())
                                .flatMap(audioPath -> audioService
                                        .sendAudioMessage(session, audioPath, sentence, isStart,
                                                isEnd)
                                        .doOnError(e -> logger.error("发送音频消息失败: {}", e.getMessage(),
                                                e)))
                                .onErrorResume(e -> {
                                    logger.error("处理句子失败: {}", e.getMessage(), e);
                                    return Mono.empty();
                                })
                                .subscribe();
                    });
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /**
     * 中止当前对话
     * 
     * @param session WebSocket会话
     * @param reason  中止原因
     * @return 处理结果
     */
    public Mono<Void> abortDialogue(WebSocketSession session, String reason) {
        String sessionId = session.getId();
        logger.info("中止对话 - SessionId: {}, Reason: {}", sessionId, reason);

        // 关闭音频流
        sessionManager.closeAudioSink(sessionId);
        sessionManager.setStreamingState(sessionId, false);

        // 终止语音发送
        return audioService.sendStop(session);
    }
}