package io.github.octopus.data.attract.common.security.service;

import io.github.octopus.data.attract.admin.api.dto.UserDTO;
import io.github.octopus.data.attract.admin.api.dto.UserInfo;
import io.github.octopus.data.attract.admin.api.feign.RemoteUserService;
import io.github.octopus.data.attract.common.core.constant.CacheConstants;
import io.github.octopus.data.attract.common.core.constant.SecurityConstants;
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
public class DataAttractUserDetailsServiceLoadByPhone implements DataAttractUserDetailsService {

	private final RemoteUserService remoteUserService;

	private final CacheManager cacheManager;

	/**
	 * 根据手机号加载用户信息
	 * @param phone 用户手机号
	 * @return 用户详细信息
	 */
	@Override
	@SneakyThrows
	public UserDetails loadUserByUsername(String phone) {
		Cache cache = cacheManager.getCache(CacheConstants.USER_DETAILS);
		if (cache != null && cache.get(phone) != null) {
			return (LoginUser) cache.get(phone).get();
		}

		UserDTO userDTO = new UserDTO();
		userDTO.setPhone(phone);
		R<UserInfo> result = remoteUserService.info(userDTO);

		UserDetails userDetails = getUserDetails(result);
		if (cache != null) {
			cache.put(phone, userDetails);
		}
		return userDetails;
	}

	/**
	 * 根据用户信息加载用户详情
	 * @param pigUser 用户信息对象
	 * @return 用户详情
	 */
	@Override
	public UserDetails loadUserByUser(LoginUser pigUser) {
		return this.loadUserByUsername(pigUser.getPhone());
	}

	/**
	 * 是否支持此客户端校验
	 * @param clientId 目标客户端
	 * @return true/false
	 */
	@Override
	public boolean support(String clientId, String grantType) {
		return SecurityConstants.MOBILE.equals(grantType);
	}

}
