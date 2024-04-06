package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendV2 {

  @JsonProperty(value = "backends")
  private List<BackendRowV2> backends;

  public List<BackendRowV2> getBackends() {
    return backends;
  }

  public void setBackends(List<BackendRowV2> backends) {
    this.backends = backends;
  }

  public static class BackendRowV2 {
    @JsonProperty("ip")
    public String ip;

    @JsonProperty("http_port")
    public int httpPort;

    @JsonProperty("is_alive")
    public boolean isAlive;

    public String getIp() {
      return ip;
    }

    public void setIp(String ip) {
      this.ip = ip;
    }

    public int getHttpPort() {
      return httpPort;
    }

    public void setHttpPort(int httpPort) {
      this.httpPort = httpPort;
    }

    public boolean isAlive() {
      return isAlive;
    }

    public void setAlive(boolean alive) {
      isAlive = alive;
    }

    public String toBackendString() {
      return ip + ":" + httpPort;
    }
  }
}
