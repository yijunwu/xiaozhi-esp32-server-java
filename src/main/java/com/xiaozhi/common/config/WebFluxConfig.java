package com.xiaozhi.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.CacheControl;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.ResourceHandlerRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import java.io.File;
import java.time.Duration;

@Configuration
@EnableWebFlux
public class WebFluxConfig implements WebFluxConfigurer {

    /**
     * 配置CORS跨域支持
     */
    @Bean
    public CorsWebFilter corsWebFilter() {
        CorsConfiguration corsConfig = new CorsConfiguration();
        corsConfig.addAllowedOrigin("*");
        corsConfig.addAllowedMethod("*");
        corsConfig.addAllowedHeader("*");
        corsConfig.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", corsConfig);

        return new CorsWebFilter(source);
    }

    /**
     * 配置静态资源处理
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        try {
            // 获取项目根目录的绝对路径
            String basePath = new File("").getAbsolutePath();

            // 音频文件存储在项目根目录下的audio文件夹中
            String audioPath = "file:" + basePath + File.separator + "audio" + File.separator;

            // 配置资源映射
            registry.addResourceHandler("/audio/**")
                    .addResourceLocations(audioPath)
                    .setCacheControl(CacheControl.maxAge(Duration.ofHours(1)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}