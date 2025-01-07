package io.github.octopus.sys.salus.model.reponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ApiResult<T> {

  private Integer code;
  private T data;
  private String message;

  private static final Integer SUCCESS_CODE = 0;
  private static final Integer FAILED_CODE = 1;

  private static final String SUCCESS_MSG = "success";
  private static final String FAILED_MSG = "failed";

  public static <T> ApiResult<T> success() {
    return new ApiResult<>(SUCCESS_CODE, null, SUCCESS_MSG);
  }

  public static <T> ApiResult<T> success(T data) {
    return new ApiResult<>(SUCCESS_CODE, data, SUCCESS_MSG);
  }

  public static <T> ApiResult<T> success(T data, String message) {
    return new ApiResult<>(SUCCESS_CODE, data, message);
  }

  public static <T> ApiResult<T> failed() {
    return new ApiResult<>(FAILED_CODE, null, FAILED_MSG);
  }

  public static <T> ApiResult<T> failed(T data) {
    return new ApiResult<>(FAILED_CODE, data, FAILED_MSG);
  }

  public static <T> ApiResult<T> failed(T data, String message) {
    return new ApiResult<>(FAILED_CODE, data, message);
  }
}
