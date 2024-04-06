package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendRow {

  @JsonProperty(value = "HttpPort")
  private String httpPort;

  @JsonProperty(value = "IP")
  private String ip;

  @JsonProperty(value = "Alive")
  private Boolean alive;
}
