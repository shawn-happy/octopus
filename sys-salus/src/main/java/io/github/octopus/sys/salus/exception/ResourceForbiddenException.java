package io.github.octopus.sys.salus.exception;

import java.text.MessageFormat;
import lombok.Getter;

@Getter
public class ResourceForbiddenException extends SalusException {

  private final String resourceType;
  private final String resource;
  private static final String ERROR_MSG_FORMAT = "{0} {1} already forbidden";

  public ResourceForbiddenException(String resourceType, String resource) {
    super(MessageFormat.format(ERROR_MSG_FORMAT, resourceType, resource));
    this.resourceType = resourceType;
    this.resource = resource;
  }

  public ResourceForbiddenException(String resourceType, String resource, Throwable cause) {
    super(MessageFormat.format(ERROR_MSG_FORMAT, resourceType, resource), cause);
    this.resourceType = resourceType;
    this.resource = resource;
  }
}
