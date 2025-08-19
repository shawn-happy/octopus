package io.github.octopus.data.attract.common.security.service;

import io.github.octopus.data.attract.admin.api.dto.UserDTO;
import io.github.octopus.data.attract.admin.api.dto.UserInfo;
import io.github.octopus.data.attract.admin.api.feign.RemoteUserService;
import io.github.octopus.data.attract.common.core.constant.CacheConstants;
import io.github.octopus.data.attract.common.core.util.R;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Primary;
import org.springframework.security.core.userdetails.UserDetails;

@Slf4j
@Primary
@RequiredArgsConstructor
public class DataAttractUserDetailsServiceLoadByUsername implements DataAttractUserDetailsService {

	private final RemoteUserService remoteUserService;

	private final CacheManager cacheManager;

	/**
	 * 根据用户名加载用户详情
	 * @param username 用户名
	 * @return 用户详情信息
	 * @throws Exception 获取用户信息过程中可能抛出的异常
	 */
	@Override
	@SneakyThrows
	public UserDetails loadUserByUsername(String username) {
		Cache cache = cacheManager.getCache(CacheConstants.USER_DETAILS);
		if (cache != null && cache.get(username) != null) {
			return (LoginUser) cache.get(username).get();
		}

		UserDTO userDTO = new UserDTO();
		userDTO.setUsername(username);
		R<UserInfo> result = remoteUserService.info(userDTO);
		UserDetails userDetails = getUserDetails(result);
		if (cache != null) {
			cache.put(username, userDetails);
		}
		return userDetails;
	}

	@Override
	public int getOrder() {
		return Integer.MIN_VALUE;
	}

}
