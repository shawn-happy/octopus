package io.github.shawn.octopus.fluxus.executor.vo;

import lombok.Getter;

@Getter
public class ApiResponse<T> {
  private int code;
  private String msg;
  private T data;

  private static final int SUCCESS = 0;
  private static final int FAILED = 1;

  public ApiResponse() {}

  public ApiResponse(int code, String msg, T data) {
    this.code = code;
    this.msg = msg;
    this.data = data;
  }

  public static <T> ApiResponse<T> ok() {
    return ok(null);
  }

  public static <T> ApiResponse<T> ok(String msg) {
    return ok(msg, null);
  }

  public static <T> ApiResponse<T> ok(T data) {
    return ok(null, data);
  }

  public static <T> ApiResponse<T> ok(String msg, T data) {
    return new ApiResponse<>(SUCCESS, msg, data);
  }

  public static <T> ApiResponse<T> error() {
    return error(null);
  }

  public static <T> ApiResponse<T> error(String msg) {
    return error(msg, null);
  }

  public static <T> ApiResponse<T> error(T data) {
    return error(null, data);
  }

  public static <T> ApiResponse<T> error(String msg, T data) {
    return new ApiResponse<>(FAILED, msg, data);
  }
}
