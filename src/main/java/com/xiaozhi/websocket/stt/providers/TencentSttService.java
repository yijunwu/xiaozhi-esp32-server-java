package com.xiaozhi.websocket.stt.providers;

import com.tencent.asrv2.SpeechRecognizer;
import com.tencent.asrv2.SpeechRecognizerListener;
import com.tencent.asrv2.SpeechRecognizerRequest;
import com.tencent.asrv2.SpeechRecognizerResponse;
import com.tencent.core.ws.Credential;
import com.tencent.core.ws.SpeechClient;
import com.xiaozhi.entity.SysConfig;
import com.xiaozhi.websocket.stt.SttService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TencentSttService implements SttService {
    private static final Logger logger = LoggerFactory.getLogger(TencentSttService.class);
    private static final String PROVIDER_NAME = "tencent";

    // 使用腾讯云SDK的默认URL
    private static final String WS_API_URL = "wss://asr.cloud.tencent.com/asr/v2/";

    private String secretId;
    private String secretKey;
    private String appId;

    // 全局共享的SpeechClient实例
    private final SpeechClient speechClient = new SpeechClient(WS_API_URL);

    // 存储当前活跃的识别会话
    private final ConcurrentHashMap<String, SpeechRecognizer> activeRecognizers = new ConcurrentHashMap<>();

    public TencentSttService(SysConfig config) {
        if (config != null) {
            this.secretId = config.getApiKey();
            this.secretKey = config.getApiSecret();
            this.appId = config.getAppId();
        }
    }

    @Override
    public String getProviderName() {
        return PROVIDER_NAME;
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public String recognition(byte[] audioData) {
        // 实现单次识别的方法...
        // 已采用了流式识别，这里不需要实现单次识别
        return null;
    }

    @Override
    public Flux<String> streamRecognition(Flux<byte[]> audioStream) {
        // 检查配置是否已设置
        if (secretId == null || secretKey == null || appId == null) {
            logger.error("腾讯云语音识别配置未设置，无法进行识别");
            return Flux.error(new IllegalStateException("腾讯云语音识别配置未设置"));
        }

        // 创建结果接收器
        Sinks.Many<String> resultSink = Sinks.many().multicast().onBackpressureBuffer();

        // 生成唯一的语音ID
        String voiceId = UUID.randomUUID().toString();

        try {
            // 创建腾讯云凭证
            Credential credential = new Credential(appId, secretId, secretKey);

            // 创建识别请求
            SpeechRecognizerRequest request = SpeechRecognizerRequest.init();
            request.setEngineModelType("16k_zh"); // 16k采样率中文模型
            request.setVoiceFormat(1); // PCM格式
            request.setVoiceId(voiceId);

            // 创建识别监听器
            SpeechRecognizerListener listener = new SpeechRecognizerListener() {
                @Override
                public void onRecognitionStart(SpeechRecognizerResponse response) {
                }

                @Override
                public void onSentenceBegin(SpeechRecognizerResponse response) {
                }

                @Override
                public void onRecognitionResultChange(SpeechRecognizerResponse response) {
                    // 非稳态结果，可能会变化
                    if (response.getResult() != null && response.getResult().getVoiceTextStr() != null) {
                        String text = response.getResult().getVoiceTextStr();
                        if (!text.isEmpty()) {
                            resultSink.tryEmitNext(text);
                        }
                    }
                }

                @Override
                public void onSentenceEnd(SpeechRecognizerResponse response) {
                    // 稳态结果，不再变化
                    if (response.getResult() != null && response.getResult().getVoiceTextStr() != null) {
                        String text = response.getResult().getVoiceTextStr();
                        if (!text.isEmpty()) {
                            resultSink.tryEmitNext(text);
                        }
                    }
                }

                @Override
                public void onRecognitionComplete(SpeechRecognizerResponse response) {
                    resultSink.tryEmitComplete();
                    // 从活跃识别器中移除
                    activeRecognizers.remove(voiceId);
                }

                @Override
                public void onFail(SpeechRecognizerResponse response) {
                    logger.error("识别失败 - VoiceId: {}, 错误: {}", voiceId,
                            response.getMessage() != null ? response.getMessage() : "未知错误");
                    resultSink.tryEmitError(new RuntimeException("识别失败: " +
                            (response.getMessage() != null ? response.getMessage() : "未知错误")));
                    // 从活跃识别器中移除
                    activeRecognizers.remove(voiceId);
                }

                @Override
                public void onMessage(SpeechRecognizerResponse response) {
                    // 可以记录所有消息，但不需要特别处理
                }
            };

            // 创建识别器
            SpeechRecognizer recognizer = new SpeechRecognizer(speechClient, credential, request, listener);

            // 存储到活跃识别器映射中
            activeRecognizers.put(voiceId, recognizer);

            // 启动识别器
            recognizer.start();

            // 标记是否已经发送了停止信号
            AtomicBoolean stopSent = new AtomicBoolean(false);

            // 订阅音频流并发送数据
            audioStream.subscribe(
                    data -> {
                        try {
                            if (activeRecognizers.containsKey(voiceId)) {
                                recognizer.write(data);
                            }
                        } catch (Exception e) {
                            logger.error("发送音频数据时发生错误 - VoiceId: {}", voiceId, e);
                            resultSink.tryEmitError(e);
                        }
                    },
                    error -> {
                        logger.error("音频流错误 - VoiceId: {}", voiceId, error);
                        resultSink.tryEmitError(error);
                        if (activeRecognizers.containsKey(voiceId)) {
                            try {
                                recognizer.stop();
                            } catch (Exception e) {
                                logger.error("停止识别器时发生错误 - VoiceId: {}", voiceId, e);
                            } finally {
                                recognizer.close();
                                activeRecognizers.remove(voiceId);
                            }
                        }
                    },
                    () -> {
                        if (activeRecognizers.containsKey(voiceId) && !stopSent.getAndSet(true)) {
                            try {
                                recognizer.stop();
                            } catch (Exception e) {
                                logger.error("停止识别器时发生错误 - VoiceId: {}", voiceId, e);
                                resultSink.tryEmitError(e);
                            }
                        }
                    });

        } catch (Exception e) {
            logger.error("创建语音识别会话时发生错误", e);
            return Flux.error(e);
        }

        return resultSink.asFlux();
    }

    // 在服务关闭时释放资源
    public void shutdown() {
        // 关闭所有活跃的识别器
        activeRecognizers.forEach((id, recognizer) -> {
            try {
                recognizer.stop();
                recognizer.close();
            } catch (Exception e) {
                logger.error("关闭识别器时发生错误 - VoiceId: {}", id, e);
            }
        });
        activeRecognizers.clear();

        // 关闭SpeechClient
        speechClient.shutdown();
    }
}
