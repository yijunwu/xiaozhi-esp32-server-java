package com.xiaozhi.websocket.stt;

import reactor.core.publisher.Flux;

/**
 * STT服务接口
 */
public interface SttService {

  /**
   * 获取服务提供商名称
   */
  String getProviderName();

  /**
   * 处理音频数据（非流式）
   * 
   * @param audioData 音频字节数组
   * @return 识别的文本结果
   */
  String recognition(byte[] audioData);

  /**
   * 流式处理音频数据
   * 
   * @param audioStream 音频数据流
   * @return 识别的文本结果流
   */
  Flux<String> streamRecognition(Flux<byte[]> audioStream);

  /**
   * 检查服务是否支持流式处理
   * 
   * @return 是否支持流式处理
   */
  default boolean supportsStreaming() {
    return false;
  }
}
