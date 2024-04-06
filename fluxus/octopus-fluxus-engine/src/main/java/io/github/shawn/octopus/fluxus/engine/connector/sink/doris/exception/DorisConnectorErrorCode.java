package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.exception;

import lombok.Getter;

@Getter
public enum DorisConnectorErrorCode {
  STREAM_LOAD_FAILED("Doris-01", "stream load error"),
  COMMIT_FAILED("Doris-02", "commit error"),
  REST_SERVICE_FAILED("Doris-03", "rest service error");

  private final String code;
  private final String description;

  DorisConnectorErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }
}
