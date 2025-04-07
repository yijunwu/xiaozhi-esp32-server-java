package com.xiaozhi.websocket.stt.factory;

import com.xiaozhi.entity.SysConfig;
import com.xiaozhi.websocket.stt.SttService;
import com.xiaozhi.websocket.stt.providers.AliyunSttService;
import com.xiaozhi.websocket.stt.providers.TencentSttService;
import com.xiaozhi.websocket.stt.providers.VoskSttService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SttServiceFactory {

  private static final Logger logger = LoggerFactory.getLogger(SttServiceFactory.class);

  // 缓存已初始化的服务
  private final Map<String, SttService> serviceCache = new ConcurrentHashMap<>();

  // 默认服务提供商名称
  private static final String DEFAULT_PROVIDER = "vosk";

  // 标记Vosk是否初始化成功
  private boolean voskInitialized = false;

  // 备选默认提供商（当Vosk初始化失败时使用）
  private String fallbackProvider = null;

  /**
   * 初始化Vosk服务（仅在第一次需要时）
   */
  private synchronized void initializeVosk() {
    if (serviceCache.containsKey(DEFAULT_PROVIDER)) {
      return;
    }

    try {
      VoskSttService voskService = new VoskSttService();
      voskService.initialize();
      serviceCache.put(DEFAULT_PROVIDER, voskService);
      voskInitialized = true;
      logger.info("Vosk STT服务初始化成功");
    } catch (Exception e) {
      logger.error("Vosk STT服务初始化失败", e);
      voskInitialized = false;
    }
  }

  /**
   * 获取默认STT服务
   * 如果Vosk可用则返回Vosk，否则返回备选服务
   */
  public SttService getDefaultSttService() {
    // 如果Vosk尚未尝试初始化，则初始化
    if (!serviceCache.containsKey(DEFAULT_PROVIDER) && !voskInitialized) {
      initializeVosk();
    }

    // 如果Vosk初始化成功，返回Vosk
    if (voskInitialized) {
      return serviceCache.get(DEFAULT_PROVIDER);
    }

    // 否则返回备选服务
    if (fallbackProvider != null && serviceCache.containsKey(fallbackProvider)) {
      return serviceCache.get(fallbackProvider);
    }

    return null;
  }

  /**
   * 根据配置获取STT服务
   */
  public SttService getSttService(SysConfig config) {
    if (config == null) {
      return getDefaultSttService();
    }

    String provider = config.getProvider();

    // 如果是Vosk且尚未初始化，则初始化
    if (DEFAULT_PROVIDER.equals(provider) && !serviceCache.containsKey(provider) && !voskInitialized) {
      initializeVosk();
    }

    // 检查是否已有该提供商的服务实例
    if (serviceCache.containsKey(provider)) {
      SttService existingService = serviceCache.get(provider);
      // 如果是API服务，可能需要更新配置
      if (!(existingService instanceof VoskSttService)) {
        // 更新API服务配置
      }
      return existingService;
    }

    // 创建新的服务实例
    try {
      SttService service;
      if (DEFAULT_PROVIDER.equals(provider)) {
        // Vosk初始化失败的情况已在前面处理
        if (!voskInitialized) {
          return null;
        }
        service = serviceCache.get(DEFAULT_PROVIDER);
      } else {
        // 创建API服务
        service = createApiService(config);
        if (service != null) {
          serviceCache.put(provider, service);

          // 如果没有备选默认服务，将此服务设为备选
          if (fallbackProvider == null) {
            fallbackProvider = provider;
          }
        }
      }
      return service;
    } catch (Exception e) {
      logger.error("创建{}服务失败", provider, e);
      return null;
    }
  }

  /**
   * 根据配置创建API类型的STT服务
   */
  private SttService createApiService(SysConfig config) {
    if (config == null) {
      return null;
    }

    String provider = config.getProvider();

    // 根据提供商类型创建对应的服务实例
    if ("tencent".equals(provider)) {
      return new TencentSttService(config);
    } else if ("aliyun".equals(provider)) {
      return new AliyunSttService(config);
    }
    // 可以添加其他服务提供商的支持

    logger.warn("不支持的STT服务提供商: {}", provider);
    return null;
  }
}