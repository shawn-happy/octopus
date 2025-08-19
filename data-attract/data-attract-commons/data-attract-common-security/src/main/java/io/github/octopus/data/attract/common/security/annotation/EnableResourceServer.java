package io.github.octopus.data.attract.common.security.annotation;

import io.github.octopus.data.attract.common.security.component.DataAttractResourceServerAutoConfiguration;
import io.github.octopus.data.attract.common.security.component.DataAttractResourceServerConfiguration;
import io.github.octopus.data.attract.common.security.feign.DataAttractFeignClientConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Documented
@Inherited
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Import({ DataAttractResourceServerAutoConfiguration.class, DataAttractResourceServerConfiguration.class,
		DataAttractFeignClientConfiguration.class })
public @interface EnableResourceServer {

}
