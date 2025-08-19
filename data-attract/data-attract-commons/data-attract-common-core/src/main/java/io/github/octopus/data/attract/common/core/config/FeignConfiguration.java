package io.github.octopus.data.attract.common.core.config;

import io.github.octopus.data.attract.common.core.feign.OctopusFeignInnerRequestInterceptor;
import io.github.octopus.data.attract.common.core.feign.OctopusFeignRequestCloseInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class FeignConfiguration {

	/**
	 * 创建并返回PigFeignRequestCloseInterceptor实例
	 * @return PigFeignRequestCloseInterceptor实例
	 */
	@Bean
	public OctopusFeignRequestCloseInterceptor octopusFeignRequestCloseInterceptor() {
		return new OctopusFeignRequestCloseInterceptor();
	}

	/**
	 * 创建并返回PigFeignInnerRequestInterceptor实例
	 * @return PigFeignInnerRequestInterceptor 内部请求拦截器实例
	 */
	@Bean
	public OctopusFeignInnerRequestInterceptor octopusFeignInnerRequestInterceptor() {
		return new OctopusFeignInnerRequestInterceptor();
	}

}
