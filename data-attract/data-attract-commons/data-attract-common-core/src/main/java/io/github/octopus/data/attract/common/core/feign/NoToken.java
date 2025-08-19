package io.github.octopus.data.attract.common.core.feign;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 服务无token调用声明注解
 * <p>
 * 只有发起方没有 token 时候才需要添加此注解， @NoToken + @Inner
 * <p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NoToken {

}
