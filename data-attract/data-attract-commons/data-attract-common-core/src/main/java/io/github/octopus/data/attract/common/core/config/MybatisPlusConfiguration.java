package io.github.octopus.data.attract.common.core.config;

import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import io.github.octopus.data.attract.common.core.db.MybatisPlusMetaObjectHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MybatisPlusConfiguration {

	/**
	 * 创建并配置MybatisPlus分页拦截器
	 * @return 配置好的MybatisPlus拦截器实例
	 */
	@Bean
	public MybatisPlusInterceptor mybatisPlusInterceptor() {
		MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
		interceptor.addInnerInterceptor(new PaginationInnerInterceptor());
		return interceptor;
	}

	/**
	 * 创建并返回MybatisPlusMetaObjectHandler实例，用于审计字段自动填充
	 * @return MybatisPlusMetaObjectHandler实例
	 */
	@Bean
	public MybatisPlusMetaObjectHandler mybatisPlusMetaObjectHandler() {
		return new MybatisPlusMetaObjectHandler();
	}

}
