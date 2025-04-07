package com.xiaozhi.websocket.service;

import com.xiaozhi.utils.OpusProcessor;
import com.xiaozhi.utils.TarsosNoiseReducer;
import com.xiaozhi.websocket.vad.impl.SileroVadModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class VadService {
    private static final Logger logger = LoggerFactory.getLogger(VadService.class);

    @Autowired
    private OpusProcessor opusDecoder;

    @Autowired
    private ResourceLoader resourceLoader;

    // 注入SileroVadModel
    @Autowired
    private SileroVadModel sileroVadModel;

    // VAD参数
    @Value("${app.vad.speech-threshold:0.5}")
    private float speechThreshold;

    @Value("${app.vad.silence-threshold:0.35}")
    private float silenceThreshold;

    @Value("${app.vad.energy-threshold:0.01}")
    private float energyThreshold;

    @Value("${app.vad.min-silence-duration:500}")
    private int minSilenceDuration;

    @Value("${app.vad.pre-buffer-duration:300}")
    private int preBufferDuration;

    @Value("${app.vad.enable-noise-reduction:true}")
    private boolean enableNoiseReduction;

    // 噪声抑制器
    private TarsosNoiseReducer tarsosNoiseReducer;

    // 会话状态管理
    private final ConcurrentHashMap<String, VadSessionState> sessionStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> sessionLocks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        try {
            // 初始化噪声抑制器
            if (enableNoiseReduction) {
                tarsosNoiseReducer = new TarsosNoiseReducer();
                logger.info("噪声抑制器初始化成功");
            }

            // 检查SileroVadModel是否已注入
            if (sileroVadModel != null) {
                logger.info("VAD服务初始化成功，使用SileroVadModel进行语音活动检测");
            } else {
                logger.error("SileroVadModel未注入，VAD功能将不可用");
            }
        } catch (Exception e) {
            logger.error("初始化VAD服务失败", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        logger.info("VAD服务资源已释放");
    }

    /**
     * 会话状态类
     */
    private class VadSessionState {
        private boolean speaking = false;
        private long lastSpeechTime = 0;
        private long lastSilenceTime = 0; // 添加最后一次检测到静音的时间
        private float averageEnergy = 0;
        private final List<Float> probabilities = new ArrayList<>();
        private final LinkedList<byte[]> preBuffer = new LinkedList<>();
        private int preBufferSize = 0; // 当前缓冲区大小（字节）
        private final int maxPreBufferSize; // 最大缓冲区大小（字节）

        public VadSessionState() {
            // 计算预缓冲区大小（16kHz, 16bit, mono = 32 bytes/ms）
            this.maxPreBufferSize = preBufferDuration * 32;
        }

        public boolean isSpeaking() {
            return speaking;
        }

        public void setSpeaking(boolean speaking) {
            this.speaking = speaking;
            if (speaking) {
                lastSpeechTime = System.currentTimeMillis();
                lastSilenceTime = 0; // 重置静音时间
            } else {
                // 如果从说话状态变为不说话，记录静音开始时间
                if (lastSilenceTime == 0) {
                    lastSilenceTime = System.currentTimeMillis();
                }
            }
        }

        public int getSilenceDuration() {
            // 如果未检测到静音，返回0
            if (lastSilenceTime == 0) {
                return 0;
            }
            // 返回从上次检测到静音到现在的时间差
            return (int) (System.currentTimeMillis() - lastSilenceTime);
        }

        // 更新静音状态
        public void updateSilenceState(boolean isSilence) {
            if (isSilence) {
                if (lastSilenceTime == 0) { // 首次检测到静音
                    lastSilenceTime = System.currentTimeMillis();
                }
            } else {
                lastSilenceTime = 0; // 检测到声音，重置静音时间
            }
        }

        public float getAverageEnergy() {
            return averageEnergy;
        }

        public void updateAverageEnergy(float currentEnergy) {
            if (averageEnergy == 0) {
                averageEnergy = currentEnergy;
            } else {
                averageEnergy = 0.95f * averageEnergy + 0.05f * currentEnergy;
            }
        }

        public void addProbability(float prob) {
            probabilities.add(prob);
            if (probabilities.size() > 10) {
                probabilities.remove(0);
            }
        }

        public float getLastProbability() {
            if (probabilities.isEmpty()) {
                return 0.0f;
            }
            return probabilities.get(probabilities.size() - 1);
        }

        public List<Float> getProbabilities() {
            return probabilities;
        }

        /**
         * 添加数据到预缓冲区
         */
        public void addToPreBuffer(byte[] data) {
            // 如果已经在说话，不需要添加到预缓冲区
            if (speaking) {
                return;
            }

            // 添加到预缓冲区
            preBuffer.add(data);
            preBufferSize += data.length;

            // 如果超出最大缓冲区大小，移除最旧的数据
            while (preBufferSize > maxPreBufferSize && !preBuffer.isEmpty()) {
                byte[] removed = preBuffer.removeFirst();
                preBufferSize -= removed.length;
            }
        }

        /**
         * 获取并清空预缓冲区数据
         */
        public byte[] drainPreBuffer() {
            if (preBuffer.isEmpty()) {
                return new byte[0];
            }

            // 计算总大小并创建结果数组
            byte[] result = new byte[preBufferSize];
            int offset = 0;

            // 复制所有缓冲区数据
            for (byte[] chunk : preBuffer) {
                System.arraycopy(chunk, 0, result, offset, chunk.length);
                offset += chunk.length;
            }

            // 清空缓冲区
            preBuffer.clear();
            preBufferSize = 0;

            return result;
        }

        /**
         * 重置状态
         */
        public void reset() {
            speaking = false;
            lastSpeechTime = 0;
            lastSilenceTime = 0;
            averageEnergy = 0;
            probabilities.clear();
            preBuffer.clear();
            preBufferSize = 0;
        }
    }

    /**
     * 初始化会话状态
     */
    public void initializeSession(String sessionId) {
        Object lock = getSessionLock(sessionId);

        synchronized (lock) {
            VadSessionState state = sessionStates.get(sessionId);
            if (state == null) {
                state = new VadSessionState();
                sessionStates.put(sessionId, state);
            } else {
                state.reset();
            }
            logger.info("VAD会话初始化 - SessionId: {}", sessionId);
        }
    }

    /**
     * 获取会话锁对象
     */
    private Object getSessionLock(String sessionId) {
        return sessionLocks.computeIfAbsent(sessionId, k -> new Object());
    }

    /**
     * 处理音频数据
     */
    public VadResult processAudio(String sessionId, byte[] opusData) {
        Object lock = getSessionLock(sessionId);

        synchronized (lock) {
            try {
                // 确保会话状态已初始化
                VadSessionState state = sessionStates.computeIfAbsent(sessionId, k -> new VadSessionState());

                // 解码Opus数据为PCM
                byte[] pcmData = opusDecoder.decodeOpusFrameToPcm(opusData);
                if (pcmData == null || pcmData.length == 0) {
                    return new VadResult(VadStatus.NO_SPEECH, null);
                }

                // 添加到预缓冲区
                state.addToPreBuffer(pcmData);

                // 应用噪声抑制
                byte[] processedPcm = applyNoiseReduction(sessionId, pcmData);

                // 计算音频能量
                float[] samples = convertBytesToFloats(processedPcm);
                float currentEnergy = calculateEnergy(samples);
                state.updateAverageEnergy(currentEnergy);

                // 执行VAD推断
                float speechProb = runVadInference(samples);
                state.addProbability(speechProb);

                // 根据VAD结果和能量判断语音状态
                boolean hasSignificantEnergy = hasSignificantEnergy(currentEnergy, state.getAverageEnergy());
                boolean isSpeech = speechProb > speechThreshold && hasSignificantEnergy;
                boolean isSilence = speechProb < silenceThreshold;

                // 更新静音状态
                state.updateSilenceState(isSilence);

                if (!state.isSpeaking() && isSpeech) {
                    // 检测到语音开始
                    state.setSpeaking(true);
                    logger.info("检测到语音开始 - SessionId: {}, 概率: {}, 能量: {}", sessionId, speechProb, currentEnergy);

                    // 获取预缓冲区数据
                    byte[] preBufferData = state.drainPreBuffer();

                    // 合并预缓冲区数据和当前数据
                    byte[] combinedData;
                    if (preBufferData.length > 0) {
                        combinedData = new byte[preBufferData.length + processedPcm.length];
                        System.arraycopy(preBufferData, 0, combinedData, 0, preBufferData.length);
                        System.arraycopy(processedPcm, 0, combinedData, preBufferData.length, processedPcm.length);
                        logger.debug("添加了{}字节的预缓冲音频 (约{}ms)", preBufferData.length, preBufferData.length / 32);
                    } else {
                        combinedData = processedPcm;
                    }

                    return new VadResult(VadStatus.SPEECH_START, combinedData);
                } else if (state.isSpeaking() && isSilence) {
                    // 检查静音持续时间
                    int silenceDuration = state.getSilenceDuration();
                    if (silenceDuration > minSilenceDuration) {
                        // 检测到语音结束
                        state.setSpeaking(false);
                        logger.info("检测到语音结束 - SessionId: {}, 静音持续: {}ms", sessionId, silenceDuration);
                        return new VadResult(VadStatus.SPEECH_END, processedPcm);
                    } else {
                        // 静音但未达到结束阈值，仍然视为语音继续
                        return new VadResult(VadStatus.SPEECH_CONTINUE, processedPcm);
                    }
                } else if (state.isSpeaking()) {
                    // 语音继续
                    return new VadResult(VadStatus.SPEECH_CONTINUE, processedPcm);
                } else {
                    // 没有检测到语音
                    return new VadResult(VadStatus.NO_SPEECH, null);
                }
            } catch (Exception e) {
                logger.error("处理音频数据失败 - SessionId: {}", sessionId, e);
                return new VadResult(VadStatus.ERROR, null);
            }
        }
    }

    /**
     * 运行VAD模型推断
     */
    private float runVadInference(float[] audioSamples) {
        if (sileroVadModel == null) {
            logger.error("SileroVadModel未注入，无法执行VAD推断");
            return 0.0f;
        }

        // 如果样本为空或长度为0，返回低概率
        if (audioSamples == null || audioSamples.length == 0) {
            return 0.0f;
        }

        try {
            // SileroVadModel需要固定大小的输入(512)
            final int requiredSize = 512;

            // 如果样本长度正好是512，直接使用
            if (audioSamples.length == requiredSize) {
                return sileroVadModel.getSpeechProbability(audioSamples);
            }

            // 如果样本长度小于512，需要填充到512
            if (audioSamples.length < requiredSize) {
                float[] paddedSamples = new float[requiredSize];
                System.arraycopy(audioSamples, 0, paddedSamples, 0, audioSamples.length);
                // 剩余部分用0填充
                for (int i = audioSamples.length; i < requiredSize; i++) {
                    paddedSamples[i] = 0.0f;
                }
                return sileroVadModel.getSpeechProbability(paddedSamples);
            }

            // 如果样本长度大于512，取中间的512个样本
            // 或者也可以分块处理并返回最大概率值
            float maxProbability = 0.0f;
            for (int offset = 0; offset <= audioSamples.length - requiredSize; offset += requiredSize / 2) { // 使用50%重叠
                float[] chunk = new float[requiredSize];
                System.arraycopy(audioSamples, offset, chunk, 0, requiredSize);
                float probability = sileroVadModel.getSpeechProbability(chunk);
                maxProbability = Math.max(maxProbability, probability);
            }

            return maxProbability;
        } catch (Exception e) {
            logger.error("VAD推断失败: {}", e.getMessage());
            return 0.0f; // 出错时返回低概率
        }
    }

    /**
     * 将PCM字节数组转换为浮点数组
     */
    private float[] convertBytesToFloats(byte[] pcmData) {
        // 16位PCM，每个样本2个字节
        int sampleCount = pcmData.length / 2;
        float[] samples = new float[sampleCount];

        // 将字节转换为16位整数，然后归一化到[-1, 1]
        ByteBuffer buffer = ByteBuffer.wrap(pcmData).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < sampleCount; i++) {
            short sample = buffer.getShort();
            samples[i] = sample / 32768.0f; // 归一化
        }

        return samples;
    }

    /**
     * 应用噪声抑制
     */
    private byte[] applyNoiseReduction(String sessionId, byte[] pcmData) {
        if (tarsosNoiseReducer != null && enableNoiseReduction) {
            return tarsosNoiseReducer.processAudio(sessionId, pcmData);
        }
        return pcmData;
    }

    /**
     * 计算音频样本的能量
     */
    private float calculateEnergy(float[] samples) {
        float energy = 0;
        for (float sample : samples) {
            energy += Math.abs(sample);
        }
        return energy / samples.length;
    }

    /**
     * 判断当前能量是否显著
     */
    private boolean hasSignificantEnergy(float currentEnergy, float averageEnergy) {
        return currentEnergy > averageEnergy * 1.5 && currentEnergy > energyThreshold;
    }

    /**
     * 重置会话状态
     */
    public void resetSession(String sessionId) {
        Object lock = getSessionLock(sessionId);

        synchronized (lock) {
            VadSessionState state = sessionStates.get(sessionId);
            if (state != null) {
                state.reset();
            }
            sessionStates.remove(sessionId);

            if (enableNoiseReduction && tarsosNoiseReducer != null) {
                tarsosNoiseReducer.cleanupSession(sessionId);
            }

            sessionLocks.remove(sessionId);
            logger.info("VAD会话重置 - SessionId: {}", sessionId);
        }
    }

    /**
     * 检查当前是否正在说话
     */
    public boolean isSpeaking(String sessionId) {
        Object lock = getSessionLock(sessionId);

        synchronized (lock) {
            VadSessionState state = sessionStates.get(sessionId);
            return state != null && state.isSpeaking();
        }
    }

    /**
     * 获取当前语音概率
     */
    public float getCurrentSpeechProbability(String sessionId) {
        Object lock = getSessionLock(sessionId);

        synchronized (lock) {
            VadSessionState state = sessionStates.get(sessionId);
            if (state != null && !state.getProbabilities().isEmpty()) {
                return state.getLastProbability();
            }
            return 0.0f;
        }
    }

    // Getter和Setter方法
    public void setSpeechThreshold(float threshold) {
        if (threshold < 0.0f || threshold > 1.0f) {
            throw new IllegalArgumentException("语音阈值必须在0.0到1.0之间");
        }
        this.speechThreshold = threshold;
        this.silenceThreshold = threshold - 0.15f;
        logger.info("VAD语音阈值已更新为: {}, 静音阈值: {}", threshold, silenceThreshold);
    }

    public void setEnergyThreshold(float threshold) {
        if (threshold < 0.0f || threshold > 1.0f) {
            throw new IllegalArgumentException("能量阈值必须在0.0到1.0之间");
        }
        this.energyThreshold = threshold;
        logger.info("能量阈值已更新为: {}", threshold);
    }

    public void setEnableNoiseReduction(boolean enable) {
        this.enableNoiseReduction = enable;
        logger.info("噪声抑制功能已{}", enable ? "启用" : "禁用");
    }

    /**
     * 设置预缓冲区持续时间（毫秒）
     */
    public void setPreBufferDuration(int durationMs) {
        if (durationMs < 0) {
            throw new IllegalArgumentException("预缓冲区持续时间不能为负值");
        }
        this.preBufferDuration = durationMs;
        logger.info("预缓冲区持续时间已更新为: {}ms", durationMs);
    }

    /**
     * VAD处理结果状态枚举
     */
    public enum VadStatus {
        NO_SPEECH, // 没有检测到语音
        SPEECH_START, // 检测到语音开始
        SPEECH_CONTINUE, // 语音继续中
        SPEECH_END, // 检测到语音结束
        ERROR // 处理错误
    }

    /**
     * VAD处理结果类
     */
    public static class VadResult {
        private final VadStatus status;
        private final byte[] processedData;

        public VadResult(VadStatus status, byte[] processedData) {
            this.status = status;
            this.processedData = processedData;
        }

        public VadStatus getStatus() {
            return status;
        }

        public byte[] getProcessedData() {
            return processedData;
        }

        public boolean isSpeechActive() {
            return status == VadStatus.SPEECH_START || status == VadStatus.SPEECH_CONTINUE;
        }

        public boolean isSpeechEnd() {
            return status == VadStatus.SPEECH_END;
        }
    }
}