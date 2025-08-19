package io.github.octopus.data.attract.admin.api.feign;

import io.github.octopus.data.attract.admin.api.dto.UserDTO;
import io.github.octopus.data.attract.admin.api.dto.UserInfo;
import io.github.octopus.data.attract.common.core.constant.ServiceNameConstants;
import io.github.octopus.data.attract.common.core.feign.NoToken;
import io.github.octopus.data.attract.common.core.util.R;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(contextId = "remoteUserService", value = ServiceNameConstants.UPMS_SERVICE)
public interface RemoteUserService {

	/**
	 * (未登录状态调用，需要加 @NoToken) 通过用户名查询用户、角色信息
	 * @param user 用户查询对象
	 * @return R
	 */
	@NoToken
	@GetMapping("/user/info/query")
	R<UserInfo> info(@SpringQueryMap UserDTO user);

}
