package io.github.octopus.sys.salus.exception;

import java.text.MessageFormat;
import lombok.Getter;

@Getter
public class ResourceNotFoundException extends SalusException {

  private final String resourceType;
  private static final String ERROR_MSG_FORMAT = "{0} {1} Not Found";

  public ResourceNotFoundException(String resourceType, String resource) {
    super(MessageFormat.format(ERROR_MSG_FORMAT, resourceType, resource));
    this.resourceType = resourceType;
  }

  public ResourceNotFoundException(String resourceType, String resource, Throwable e) {
    super(MessageFormat.format(ERROR_MSG_FORMAT, resourceType, resource), e);
    this.resourceType = resourceType;
  }
}
