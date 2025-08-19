package io.github.octopus.data.attract.common.security.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.octopus.data.attract.common.core.constant.CommonConstants;
import io.github.octopus.data.attract.common.core.util.R;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import org.springframework.security.web.AuthenticationEntryPoint;

import java.io.PrintWriter;

@RequiredArgsConstructor
public class ResourceAuthExceptionEntryPoint implements AuthenticationEntryPoint {

	private final ObjectMapper objectMapper;

	private final MessageSource messageSource;

	/**
	 * 处理认证失败的响应
	 * @param request HTTP请求
	 * @param response HTTP响应
	 * @param authException 认证异常
	 * @throws Exception 写入响应时可能抛出异常
	 */
	@Override
	@SneakyThrows
	public void commence(HttpServletRequest request, HttpServletResponse response,
			AuthenticationException authException) {
		response.setCharacterEncoding(CommonConstants.UTF8);
		response.setContentType(CommonConstants.CONTENT_TYPE);
		R<String> result = new R<>();
		result.setCode(CommonConstants.FAIL);
		response.setStatus(HttpStatus.UNAUTHORIZED.value());
		if (authException != null) {
			result.setMsg("error");
			result.setData(authException.getMessage());
		}

		// 针对令牌过期返回特殊的 424
		if (authException instanceof InvalidBearerTokenException
				|| authException instanceof InsufficientAuthenticationException) {
			response.setStatus(org.springframework.http.HttpStatus.FAILED_DEPENDENCY.value());
			result.setMsg(this.messageSource.getMessage("OAuth2ResourceOwnerBaseAuthenticationProvider.tokenExpired",
					null, LocaleContextHolder.getLocale()));
		}
		PrintWriter printWriter = response.getWriter();
		printWriter.append(objectMapper.writeValueAsString(result));
	}

}
