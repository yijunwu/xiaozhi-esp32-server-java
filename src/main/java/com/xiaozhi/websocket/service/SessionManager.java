package com.xiaozhi.websocket.service;

import com.xiaozhi.entity.SysConfig;
import com.xiaozhi.entity.SysDevice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket会话管理服务
 * 负责管理所有WebSocket连接的会话状态
 */
@Service
public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    // 用于存储所有连接的会话
    private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    // 用于存储会话和设备的映射关系
    private final ConcurrentHashMap<String, SysDevice> deviceConfigs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, SysConfig> configCache = new ConcurrentHashMap<>();

    // 用于跟踪会话是否处于监听状态
    private final ConcurrentHashMap<String, Boolean> listeningState = new ConcurrentHashMap<>();

    // 用于存储每个会话的音频数据流
    private final ConcurrentHashMap<String, Sinks.Many<byte[]>> audioSinks = new ConcurrentHashMap<>();

    // 用于跟踪会话是否正在进行流式识别
    private final ConcurrentHashMap<String, Boolean> streamingState = new ConcurrentHashMap<>();

    /**
     * 注册新的WebSocket会话
     * 
     * @param sessionId 会话ID
     * @param session   WebSocket会话
     */
    public void registerSession(String sessionId, WebSocketSession session) {
        sessions.put(sessionId, session);
        listeningState.put(sessionId, false);
        streamingState.put(sessionId, false);
        logger.info("WebSocket会话已注册 - SessionId: {}", sessionId);
    }

    /**
     * 关闭并清理WebSocket会话
     * 
     * @param sessionId 会话ID
     */
    public void closeSession(String sessionId) {
        sessions.remove(sessionId);
        SysDevice device = deviceConfigs.remove(sessionId);
        listeningState.remove(sessionId);
        streamingState.remove(sessionId);

        // 清理音频流
        Sinks.Many<byte[]> sink = audioSinks.remove(sessionId);
        if (sink != null) {
            sink.tryEmitComplete();
        }

        logger.info("WebSocket会话已关闭 - SessionId: {}", sessionId);
    }

    /**
     * 注册设备配置
     * 
     * @param sessionId 会话ID
     * @param device    设备信息
     */
    public void registerDevice(String sessionId, SysDevice device) {
        deviceConfigs.put(sessionId, device);
        logger.debug("设备配置已注册 - SessionId: {}, DeviceId: {}", sessionId, device.getDeviceId());
    }

    /**
     * 缓存配置信息
     * 
     * @param configId 配置ID
     * @param config   配置信息
     */
    public void cacheConfig(Integer configId, SysConfig config) {
        if (configId != null && config != null) {
            configCache.put(configId, config);
        }
    }

    /**
     * 获取会话
     * 
     * @param sessionId 会话ID
     * @return WebSocket会话
     */
    public WebSocketSession getSession(String sessionId) {
        return sessions.get(sessionId);
    }

    /**
     * 获取设备配置
     * 
     * @param sessionId 会话ID
     * @return 设备配置
     */
    public SysDevice getDeviceConfig(String sessionId) {
        return deviceConfigs.get(sessionId);
    }

    /**
     * 获取缓存的配置
     * 
     * @param configId 配置ID
     * @return 配置信息
     */
    public SysConfig getCachedConfig(Integer configId) {
        return configCache.get(configId);
    }

    /**
     * 设置监听状态
     * 
     * @param sessionId   会话ID
     * @param isListening 是否正在监听
     */
    public void setListeningState(String sessionId, boolean isListening) {
        listeningState.put(sessionId, isListening);
    }

    /**
     * 获取监听状态
     * 
     * @param sessionId 会话ID
     * @return 是否正在监听
     */
    public boolean isListening(String sessionId) {
        return listeningState.getOrDefault(sessionId, false);
    }

    /**
     * 设置流式识别状态
     * 
     * @param sessionId   会话ID
     * @param isStreaming 是否正在流式识别
     */
    public void setStreamingState(String sessionId, boolean isStreaming) {
        streamingState.put(sessionId, isStreaming);
    }

    /**
     * 获取流式识别状态
     * 
     * @param sessionId 会话ID
     * @return 是否正在流式识别
     */
    public boolean isStreaming(String sessionId) {
        return streamingState.getOrDefault(sessionId, false);
    }

    /**
     * 创建并注册音频数据接收器
     * 
     * @param sessionId 会话ID
     * @return 音频数据接收器
     */
    public Sinks.Many<byte[]> createAudioSink(String sessionId) {
        Sinks.Many<byte[]> sink = Sinks.many().multicast().onBackpressureBuffer();
        audioSinks.put(sessionId, sink);
        return sink;
    }

    /**
     * 获取音频数据接收器
     * 
     * @param sessionId 会话ID
     * @return 音频数据接收器
     */
    public Sinks.Many<byte[]> getAudioSink(String sessionId) {
        return audioSinks.get(sessionId);
    }

    /**
     * 关闭音频数据接收器
     * 
     * @param sessionId 会话ID
     */
    public void closeAudioSink(String sessionId) {
        Sinks.Many<byte[]> sink = audioSinks.get(sessionId);
        if (sink != null) {
            sink.tryEmitComplete();
        }
    }
}