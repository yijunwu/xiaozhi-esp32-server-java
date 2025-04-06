package com.xiaozhi.websocket.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xiaozhi.entity.SysConfig;
import com.xiaozhi.entity.SysDevice;
import com.xiaozhi.service.SysConfigService;
import com.xiaozhi.service.SysDeviceService;
import com.xiaozhi.websocket.llm.LlmManager;
import com.xiaozhi.websocket.service.AudioService;
import com.xiaozhi.websocket.service.MessageService;
import com.xiaozhi.websocket.service.SpeechToTextService;
import com.xiaozhi.websocket.service.TextToSpeechService;
import com.xiaozhi.websocket.service.VadService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReactiveWebSocketHandler implements WebSocketHandler {

    @Autowired
    private SysDeviceService deviceService;

    @Autowired
    private SysConfigService configService;

    @Autowired
    private AudioService audioService;

    @Autowired
    private LlmManager llmManager;

    @Autowired
    private MessageService messageService;

    @Autowired
    private TextToSpeechService textToSpeechService;

    @Autowired
    private SpeechToTextService speechToTextService;

    @Autowired
    private VadService vadService;

    private static final Logger logger = LoggerFactory.getLogger(ReactiveWebSocketHandler.class);

    // 用于存储所有连接的会话
    private static final ConcurrentHashMap<String, WebSocketSession> SESSIONS = new ConcurrentHashMap<>();

    // 用于存储会话和设备的映射关系
    private static final ConcurrentHashMap<String, SysDevice> DEVICES_CONFIG = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, SysConfig> CONFIG = new ConcurrentHashMap<>();

    // 用于跟踪会话是否处于监听状态
    private static final ConcurrentHashMap<String, Boolean> LISTENING_STATE = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DataBufferFactory bufferFactory = new DefaultDataBufferFactory();

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String sessionId = session.getId();
        SESSIONS.put(sessionId, session);
        LISTENING_STATE.put(sessionId, false);

        logger.info(session.getHandshakeInfo().getHeaders().toString());

        // 从请求头中获取设备ID
        String deviceId = session.getHandshakeInfo().getHeaders().getFirst("device-Id");
        if (deviceId == null) {
            logger.error("设备ID为空");
            return session.close();
        }

        return Mono.fromCallable(() -> deviceService.query(new SysDevice().setDeviceId(deviceId)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(devices -> {
                    SysDevice device;
                    if (devices.isEmpty()) {
                        device = new SysDevice();
                        device.setDeviceId(deviceId);
                        device.setSessionId(sessionId);
                    } else {
                        device = devices.get(0);
                        device.setSessionId(sessionId);
                        if (device.getSttId() != null) {
                            CONFIG.put(device.getSttId(), configService.selectConfigById(device.getSttId()));
                        }
                        if (device.getTtsId() != null) {
                            CONFIG.put(device.getTtsId(), configService.selectConfigById(device.getTtsId()));
                        }
                    }
                    DEVICES_CONFIG.put(sessionId, device);
                    logger.info("WebSocket连接建立成功 - SessionId: {}, DeviceId: {}", sessionId, deviceId);

                    // 更新设备状态
                    return Mono.fromRunnable(() -> deviceService.update(new SysDevice()
                            .setDeviceId(device.getDeviceId())
                            .setState("1")
                            .setLastLogin(new Date().toString()))).subscribeOn(Schedulers.boundedElastic()).then();
                })
                .then(
                        // 处理接收到的消息
                        session.receive()
                                .flatMap(message -> {
                                    if (message.getType() == WebSocketMessage.Type.TEXT) {
                                        return handleTextMessage(session, message);
                                    } else if (message.getType() == WebSocketMessage.Type.BINARY) {
                                        return handleBinaryMessage(session, message);
                                    }
                                    return Mono.empty();
                                })
                                .onErrorResume(e -> {
                                    logger.error("处理WebSocket消息失败", e);
                                    return Mono.empty();
                                })
                                .then())
                .doFinally(signal -> {
                    // 连接关闭时清理资源
                    SysDevice device = DEVICES_CONFIG.get(sessionId);
                    if (device != null) {
                        deviceService.update(new SysDevice()
                                .setDeviceId(device.getDeviceId())
                                .setState("0")
                                .setLastLogin(new Date().toString()));
                        logger.info("WebSocket连接关闭 - SessionId: {}, DeviceId: {}", sessionId, device.getDeviceId());
                    }

                    SESSIONS.remove(sessionId);
                    DEVICES_CONFIG.remove(sessionId);
                    LISTENING_STATE.remove(sessionId);

                    // 清理VAD会话
                    vadService.resetSession(sessionId);

                    // 清理音频处理会话
                    audioService.cleanupSession(sessionId);
                });
    }

    private Mono<Void> handleTextMessage(WebSocketSession session, WebSocketMessage message) {
        String sessionId = session.getId();
        SysDevice device = DEVICES_CONFIG.get(sessionId);
        String payload = message.getPayloadAsText();

        try {
            // 首先尝试解析JSON消息
            JsonNode jsonNode = objectMapper.readTree(payload);
            String messageType = jsonNode.path("type").asText();

            // hello消息应该始终处理，无论设备是否绑定
            if ("hello".equals(messageType)) {
                return handleHelloMessage(session, jsonNode);
            }

            // 对于其他消息类型，需要检查设备是否已绑定
            return Mono.fromCallable(
                    () -> deviceService
                            .query(new SysDevice().setDeviceId(device.getDeviceId()).setSessionId(sessionId)))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(deviceResult -> {
                        if (deviceResult.isEmpty()) {
                            // 设备未绑定，处理未绑定设备的消息
                            return handleUnboundDevice(session, device);
                        } else {
                            // 设备已绑定，根据消息类型处理
                            switch (messageType) {
                                case "listen":
                                    return handleListenMessage(session, jsonNode);
                                case "abort":
                                    return handleAbortMessage(session, jsonNode);
                                case "iot":
                                    return handleIotMessage(session, jsonNode);
                                default:
                                    logger.warn("未知的消息类型: {}", messageType);
                                    return Mono.empty();
                            }
                        }
                    });
        } catch (Exception e) {
            logger.error("处理文本消息失败", e);
            return Mono.empty();
        }
    }

    private Mono<Void> handleBinaryMessage(WebSocketSession session, WebSocketMessage message) {
        String sessionId = session.getId();
        SysDevice device = DEVICES_CONFIG.get(sessionId);
        SysConfig sttConfig = (device.getSttId() != null) ? CONFIG.get(device.getSttId()) : null;
        SysConfig ttsConfig = (device.getTtsId() != null) ? CONFIG.get(device.getTtsId()) : null;

        // 检查会话是否处于监听状态，如果不是则忽略音频数据
        Boolean isListening = LISTENING_STATE.getOrDefault(sessionId, false);
        if (!isListening) {
            return Mono.empty();
        }

        // 获取二进制数据
        DataBuffer dataBuffer = message.getPayload();

        // 修复：使用DataBufferUtils.retain()增加引用计数，防止过早释放
        DataBuffer retainedBuffer = DataBufferUtils.retain(dataBuffer);

        // 从缓冲区复制数据到字节数组（在主线程中完成，避免并发访问）
        byte[] opusData = new byte[retainedBuffer.readableByteCount()];
        retainedBuffer.read(opusData);
        DataBufferUtils.release(retainedBuffer); // 立即释放缓冲区

        // 使用单个Mono处理音频数据，避免链式调用中的并发问题
        return Mono.fromCallable(() -> {
            try {
                // 使用改进的VadService处理音频数据
                return vadService.processAudio(sessionId, opusData);
            } catch (Exception e) {
                logger.error("VAD处理异常: {}", e.getMessage(), e);
                return null;
            }
        })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(completeAudio -> {
                    if (completeAudio == null) {
                        // 如果没有完整的音频数据（语音未结束），直接返回空
                        return Mono.<Void>empty();
                    }

                    logger.info("检测到语音结束 - SessionId: {}, 音频大小: {} 字节", sessionId, completeAudio.length);

                    // 进行语音识别
                    return Mono.fromCallable(() -> {
                        String result;
                        // 调用 SpeechToTextService 进行语音识别
                        if (!ObjectUtils.isEmpty(sttConfig)) {
                            result = speechToTextService.recognition(completeAudio, sttConfig);
                        } else {
                            String jsonResult = speechToTextService.recognition(completeAudio);
                            try {
                                JsonNode resultNode = objectMapper.readTree(jsonResult);
                                result = resultNode.path("text").asText("");
                            } catch (Exception e) {
                                logger.error("解析语音识别结果失败", e);
                                result = "";
                            }
                        }
                        return result;
                    })
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(recognizedText -> {
                                if (!StringUtils.hasText(recognizedText)) {
                                    return Mono.empty();
                                }

                                logger.info("语音识别结果 - SessionId: {}, 内容: {}", sessionId, recognizedText);

                                // 设置会话为非监听状态，防止处理自己的声音
                                LISTENING_STATE.put(sessionId, false);

                                // 发送识别结果
                                return messageService.sendMessage(session, "stt", "start", recognizedText)
                                        .then(Mono.fromRunnable(() -> {
                                            // 使用句子切分处理流式响应
                                            llmManager.chatStreamBySentence(device, recognizedText,
                                                    (sentence, isStart, isEnd) -> {
                                                        // 为每个句子创建一个独立的响应式流并订阅它
                                                        Mono.fromCallable(() -> textToSpeechService.textToSpeech(
                                                                sentence, ttsConfig, device.getVoiceName()))
                                                                .subscribeOn(Schedulers.boundedElastic())
                                                                .flatMap(audioPath -> {if (audioPath != null) return audioService
                                                                        .sendAudioMessage(session, audioPath, sentence,
                                                                                isStart, isEnd)
                                                                        .doOnError(e -> logger.error("发送音频消息失败: {}", e.getMessage(), e)); else return null;})
                                                                .onErrorResume(e -> {
                                                                    logger.error("处理句子失败: {}", e.getMessage(), e);
                                                                    return Mono.empty();
                                                                })
                                                                .subscribe();
                                                    });
                                        }));
                            });
                })
                .onErrorResume(e -> {
                    logger.error("处理音频数据失败: {}", e.getMessage(), e);
                    return Mono.empty();
                });
    }

    private Mono<Void> handleUnboundDevice(WebSocketSession session, SysDevice device) {
        return Mono.fromCallable(() -> {
            SysDevice codeResult = deviceService.generateCode(device);
            String audioFilePath;
            if (!StringUtils.hasText(codeResult.getAudioPath())) {
                audioFilePath = textToSpeechService.textToSpeech("请到设备管理页面添加设备，输入验证码" + codeResult.getCode());
                codeResult.setDeviceId(device.getDeviceId());
                codeResult.setSessionId(session.getId());
                codeResult.setAudioPath(audioFilePath);
                deviceService.updateCode(codeResult);
            } else {
                audioFilePath = codeResult.getAudioPath();
            }
            logger.info("设备未绑定，返回验证码");
            return audioService.sendAudioMessage(session, audioFilePath, codeResult.getCode(), true, true);
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private Mono<Void> handleHelloMessage(WebSocketSession session, JsonNode jsonNode) {
        logger.info("收到hello消息 - SessionId: {}", session.getId());

        // 验证客户端hello消息
        if (!jsonNode.path("transport").asText().equals("websocket")) {
            logger.warn("不支持的传输方式: {}", jsonNode.path("transport").asText());
            return session.close();
        }

        // 解析音频参数
        JsonNode audioParams = jsonNode.path("audio_params");
        String format = audioParams.path("format").asText();
        int sampleRate = audioParams.path("sample_rate").asInt();
        int channels = audioParams.path("channels").asInt();
        int frameDuration = audioParams.path("frame_duration").asInt();

        logger.info("客户端音频参数 - 格式: {}, 采样率: {}, 声道: {}, 帧时长: {}ms",
                format, sampleRate, channels, frameDuration);

        // 回复hello消息
        ObjectNode response = objectMapper.createObjectNode();
        response.put("type", "hello");
        response.put("transport", "websocket");

        // 添加音频参数（可以根据服务器配置调整）
        ObjectNode responseAudioParams = response.putObject("audio_params");
        responseAudioParams.put("format", format);
        responseAudioParams.put("sample_rate", sampleRate);
        responseAudioParams.put("channels", channels);
        responseAudioParams.put("frame_duration", frameDuration);

        return session.send(Mono.just(session.textMessage(response.toString())));
    }

    private Mono<Void> handleListenMessage(WebSocketSession session, JsonNode jsonNode) {
        String sessionId = session.getId();
        SysDevice device = DEVICES_CONFIG.get(sessionId);
        SysConfig sttConfig = (device.getSttId() != null) ? CONFIG.get(device.getSttId()) : null;
        SysConfig ttsConfig = (device.getTtsId() != null) ? CONFIG.get(device.getTtsId()) : null;

        // 解析listen消息中的state和mode字段
        String state = jsonNode.path("state").asText();
        String mode = jsonNode.path("mode").asText();

        logger.info("收到listen消息 - SessionId: {}, State: {}, Mode: {}", sessionId, state, mode);

        // 根据state处理不同的监听状态
        switch (state) {
            case "start":
                // 开始监听，准备接收音频数据
                logger.info("开始监听 - Mode: {}", mode);
                LISTENING_STATE.put(sessionId, true);
                return Mono.empty();
            case "stop":
                // 停止监听
                logger.info("停止监听");
                LISTENING_STATE.put(sessionId, false);
                return Mono.empty();
            case "detect":
                // 检测到唤醒词
                String text = jsonNode.path("text").asText();
                logger.info("检测到唤醒词: {}", text);

                // 设置为非监听状态，防止处理自己的声音
                LISTENING_STATE.put(sessionId, false);

                // 发送识别结果
                messageService.sendMessage(session, "stt", "start", text).subscribe();

                // 使用句子切分处理流式响应
                return Mono.fromRunnable(() -> {
                    llmManager.chatStreamBySentence(device, text, (sentence, isStart, isEnd) -> {
                        Mono.fromCallable(
                                () -> textToSpeechService.textToSpeech(sentence, ttsConfig, device.getVoiceName()))
                                .subscribeOn(Schedulers.boundedElastic())
                                .flatMap(audioPath -> audioService
                                        .sendAudioMessage(session, audioPath, sentence, isStart, isEnd)
                                        .doOnError(e -> logger.error("发送音频消息失败: {}", e.getMessage(), e)))
                                .onErrorResume(e -> {
                                    logger.error("处理句子失败: {}", e.getMessage(), e);
                                    return Mono.empty();
                                })
                                .subscribe(); // 订阅以确保执行
                    });
                }).subscribeOn(Schedulers.boundedElastic()).then();
            default:
                logger.warn("未知的listen状态: {}", state);
                return Mono.empty();
        }
    }

    private Mono<Void> handleAbortMessage(WebSocketSession session, JsonNode jsonNode) {
        String sessionId = session.getId();
        String reason = jsonNode.path("reason").asText();

        logger.info("收到abort消息 - SessionId: {}, Reason: {}", sessionId, reason);

        // 终止语音发送
        return audioService.sendStop(session);
    }

    private Mono<Void> handleIotMessage(WebSocketSession session, JsonNode jsonNode) {
        String sessionId = session.getId();
        logger.info("收到IoT消息 - SessionId: {}", sessionId);

        // 处理设备描述信息
        if (jsonNode.has("descriptors")) {
            JsonNode descriptors = jsonNode.path("descriptors");
            logger.info("收到设备描述信息: {}", descriptors);
            // 处理设备描述信息的逻辑
        }

        // 处理设备状态更新
        if (jsonNode.has("states")) {
            JsonNode states = jsonNode.path("states");
            logger.info("收到设备状态更新: {}", states);
            // 处理设备状态更新的逻辑
        }

        return Mono.empty();
    }

}