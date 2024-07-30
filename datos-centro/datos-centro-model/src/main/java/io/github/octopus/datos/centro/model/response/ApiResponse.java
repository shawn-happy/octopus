package io.github.octopus.datos.centro.model.response;

import lombok.Getter;

@Getter
public class ApiResponse<T> {

  private int code;
  private T data;
  private String message;

  public ApiResponse() {}

  public ApiResponse(int code, T data, String message) {
    this.code = code;
    this.data = data;
    this.message = message;
  }

  public static <T> ApiResponse<T> success() {
    return new ApiResponse<>(1, null, "success");
  }

  public static <T> ApiResponse<T> error() {
    return new ApiResponse<>(0, null, "error");
  }

  public static <T> ApiResponse<T> error(int code, String msg) {
    return new ApiResponse<>(code, null, msg);
  }

  public static <T> ApiResponse<T> error(String msg) {
    return new ApiResponse<>(0, null, msg);
  }
}
