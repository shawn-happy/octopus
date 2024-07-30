package io.github.octopus.datos.centro.config;

import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.model.response.ApiResponse;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  /** 处理平台自定义异常 */
  @ExceptionHandler(DataCenterServiceException.class)
  public ApiResponse<?> platformException(DataCenterServiceException e) {
    log.error("PlatformException，原因{}", e.getMessage());
    return ApiResponse.error(0, e.getMessage());
  }

  /** 处理 json 请求体调用接口对象参数校验失败抛出的异常 */
  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ApiResponse<?> jsonParamsException(MethodArgumentNotValidException e) {
    BindingResult bindingResult = e.getBindingResult();

    StringBuilder builder = new StringBuilder();

    for (FieldError fieldError : bindingResult.getFieldErrors()) {
      String msg = String.format("%s%s;", fieldError.getField(), fieldError.getDefaultMessage());
      builder.append(msg);
    }

    return ApiResponse.error(2, builder.toString());
  }

  /** 处理单个参数校验失败抛出的异常 */
  @ExceptionHandler(ConstraintViolationException.class)
  public ApiResponse<?> ParamsException(ConstraintViolationException e) {
    StringBuilder builder = new StringBuilder();
    Set<ConstraintViolation<?>> violations = e.getConstraintViolations();
    for (ConstraintViolation<?> violation : violations) {
      StringBuilder message = new StringBuilder();
      Path path = violation.getPropertyPath();
      String[] pathArr = StringUtils.split(path.toString(), ".");

      String msg =
          message
              .append(pathArr != null ? pathArr[1] : 0)
              .append(violation.getMessage())
              .toString();
      builder.append(msg);
    }
    return ApiResponse.error(builder.toString());
  }

  /**
   * @return 处理 form data方式调用接口对象参数校验失败抛出的异常
   */
  @ExceptionHandler(BindException.class)
  public ApiResponse<?> formDaraParamsException(BindException e) {
    List<FieldError> fieldErrors = e.getBindingResult().getFieldErrors();
    String msg =
        fieldErrors.stream()
            .map(o -> o.getField() + o.getDefaultMessage())
            .collect(Collectors.toList())
            .toString();

    return ApiResponse.error(msg);
  }

  @ExceptionHandler(Exception.class)
  public ApiResponse<?> otherException(Exception e) {
    log.error("异常:", e);
    return ApiResponse.error(e.getMessage());
  }
}
