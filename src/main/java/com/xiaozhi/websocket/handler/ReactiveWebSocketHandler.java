package com.xiaozhi.websocket.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xiaozhi.entity.SysConfig;
import com.xiaozhi.entity.SysDevice;
import com.xiaozhi.service.SysConfigService;
import com.xiaozhi.service.SysDeviceService;
import com.xiaozhi.websocket.service.AudioService;
import com.xiaozhi.websocket.service.DialogueService;
import com.xiaozhi.websocket.service.SessionManager;
import com.xiaozhi.websocket.service.VadService;
import com.xiaozhi.websocket.tts.factory.TtsServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Date;

@Component
public class ReactiveWebSocketHandler implements WebSocketHandler {

    @Autowired
    private SysDeviceService deviceService;

    @Autowired
    private SysConfigService configService;

    @Autowired
    private AudioService audioService;

    @Autowired
    private TtsServiceFactory ttsService;

    @Autowired
    private VadService vadService;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private DialogueService dialogueService;

    private static final Logger logger = LoggerFactory.getLogger(ReactiveWebSocketHandler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String sessionId = session.getId();
        // 注册会话
        sessionManager.registerSession(sessionId, session);
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
                            SysConfig sttConfig = configService.selectConfigById(device.getSttId());
                            sessionManager.cacheConfig(device.getSttId(), sttConfig);
                        }
                        if (device.getTtsId() != null) {
                            SysConfig ttsConfig = configService.selectConfigById(device.getTtsId());
                            sessionManager.cacheConfig(device.getTtsId(), ttsConfig);
                        }
                    }
                    sessionManager.registerDevice(sessionId, device);
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
                    SysDevice device = sessionManager.getDeviceConfig(sessionId);
                    if (device != null) {
                        deviceService.update(new SysDevice()
                                .setDeviceId(device.getDeviceId())
                                .setState("0")
                                .setLastLogin(new Date().toString()));
                        logger.info("WebSocket连接关闭 - SessionId: {}, DeviceId: {}", sessionId, device.getDeviceId());
                    }

                    // 清理会话
                    sessionManager.closeSession(sessionId);
                    // 清理VAD会话
                    vadService.resetSession(sessionId);

                    // 清理音频处理会话
                    audioService.cleanupSession(sessionId);
                });
    }

    private Mono<Void> handleTextMessage(WebSocketSession session, WebSocketMessage message) {
        String sessionId = session.getId();
        SysDevice device = sessionManager.getDeviceConfig(sessionId);
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
                                    return dialogueService.abortDialogue(session, jsonNode.path("reason").asText());
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
        // 获取二进制数据
        DataBuffer dataBuffer = message.getPayload();
        DataBuffer retainedBuffer = DataBufferUtils.retain(dataBuffer);
        byte[] opusData = new byte[retainedBuffer.readableByteCount()];
        retainedBuffer.read(opusData);
        DataBufferUtils.release(retainedBuffer);

        // 委托给DialogueService处理音频数据
        return dialogueService.processAudioData(session, opusData);
    }

    private Mono<Void> handleUnboundDevice(WebSocketSession session, SysDevice device) {
        return Mono.fromCallable(() -> {
            SysDevice codeResult = deviceService.generateCode(device);
            String audioFilePath;
            if (!StringUtils.hasText(codeResult.getAudioPath())) {
                audioFilePath = ttsService.getTtsService().textToSpeech("请到设备管理页面添加设备，输入验证码" + codeResult.getCode());
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
        // 解析listen消息中的state和mode字段
        String state = jsonNode.path("state").asText();
        String mode = jsonNode.path("mode").asText();

        logger.info("收到listen消息 - SessionId: {}, State: {}, Mode: {}", sessionId, state, mode);

        // 根据state处理不同的监听状态
        switch (state) {
            case "start":
                // 开始监听，准备接收音频数据
                logger.info("开始监听 - Mode: {}", mode);
                sessionManager.setListeningState(sessionId, true);

                // 初始化VAD会话
                vadService.initializeSession(sessionId);

                return Mono.empty();
            case "stop":
                // 停止监听
                logger.info("停止监听");
                sessionManager.setListeningState(sessionId, false);

                // 关闭音频流
                sessionManager.closeAudioSink(sessionId);
                sessionManager.setStreamingState(sessionId, false);
                // 重置VAD会话
                vadService.resetSession(sessionId);

                return Mono.empty();
            case "detect":
                // 检测到唤醒词
                String text = jsonNode.path("text").asText();
                return dialogueService.handleWakeWord(session, text);
            default:
                logger.warn("未知的listen状态: {}", state);
                return Mono.empty();
        }
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