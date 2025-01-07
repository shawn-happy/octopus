package io.github.octopus.sys.salus.exception;

import lombok.Getter;

@Getter
public class DeptNotFoundException extends ResourceNotFoundException {

  private final String dept;

  public DeptNotFoundException(String dept) {
    super("dept", dept);
    this.dept = dept;
  }
}
