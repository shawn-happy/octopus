package io.github.octopus.data.attract.admin.api.feign;

import io.github.octopus.data.attract.common.core.constant.ServiceNameConstants;
import io.github.octopus.data.attract.common.core.feign.NoToken;
import io.github.octopus.data.attract.common.core.util.R;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(contextId = "remoteParamService", value = ServiceNameConstants.UPMS_SERVICE)
public interface RemoteParamService {

	/**
	 * 通过key 查询参数配置
	 * @param key key
	 * @NoToken 声明成内部调用，避免MQ 等无法调用
	 */
	@NoToken
	@GetMapping("/param/publicValue/{key}")
	R<String> getByKey(@PathVariable("key") String key);

}
