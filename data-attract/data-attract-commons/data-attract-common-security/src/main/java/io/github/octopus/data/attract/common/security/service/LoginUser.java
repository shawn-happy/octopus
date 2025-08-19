package io.github.octopus.data.attract.common.security.service;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Getter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.SpringSecurityCoreVersion;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.oauth2.core.OAuth2AuthenticatedPrincipal;

import java.io.Serial;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LoginUser extends User implements OAuth2AuthenticatedPrincipal {

	@Serial
	private static final long serialVersionUID = SpringSecurityCoreVersion.SERIAL_VERSION_UID;

	/**
	 * 扩展属性，方便存放oauth 上下文相关信息
	 */
	private final Map<String, Object> attributes = new HashMap<>();

	/**
	 * 用户ID
	 */
	@Getter
	@JsonSerialize(using = ToStringSerializer.class)
	private final Long id;

	/**
	 * 部门ID
	 */
	@Getter
	@JsonSerialize(using = ToStringSerializer.class)
	private final Long deptId;

	/**
	 * 手机号
	 */
	@Getter
	private final String phone;

	public LoginUser(Long id, Long deptId, String username, String password, String phone, boolean enabled,
			boolean accountNonExpired, boolean credentialsNonExpired, boolean accountNonLocked,
			Collection<? extends GrantedAuthority> authorities) {
		super(username, password, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked, authorities);
		this.id = id;
		this.deptId = deptId;
		this.phone = phone;
	}

	/**
	 * 获取OAuth 2.0令牌属性
	 * @return OAuth 2.0令牌属性Map
	 */
	@Override
	public Map<String, Object> getAttributes() {
		return this.attributes;
	}

	/**
	 * 获取用户名称
	 * @return 用户名称
	 */
	@Override
	public String getName() {
		return this.getUsername();
	}

}
