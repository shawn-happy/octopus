package io.github.octopus.sys.salus.exception;

import lombok.Getter;

@Getter
public class DeptAlreadyExistsException extends ResourceAlreadyExistsException {

  private final String dept;

  public DeptAlreadyExistsException(String dept) {
    super("dept", dept);
    this.dept = dept;
  }

  public DeptAlreadyExistsException(String dept, Throwable cause) {
    super("dept", dept, cause);
    this.dept = dept;
  }
}
