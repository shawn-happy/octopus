package io.github.octopus.sys.salus.exception;

import lombok.Getter;

@Getter
public class DeptAlreadyForbiddenException extends ResourceForbiddenException {

  private final String dept;

  public DeptAlreadyForbiddenException(String dept) {
    super("dept", dept);
    this.dept = dept;
  }

  public DeptAlreadyForbiddenException(String dept, Throwable cause) {
    super("dept", dept, cause);
    this.dept = dept;
  }
}
