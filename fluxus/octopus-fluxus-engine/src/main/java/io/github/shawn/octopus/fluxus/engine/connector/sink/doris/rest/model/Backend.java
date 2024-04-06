package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Be response model */
@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class Backend {

  @JsonProperty(value = "rows")
  private List<BackendRow> rows;

  public List<BackendRow> getRows() {
    return rows;
  }

  public void setRows(List<BackendRow> rows) {
    this.rows = rows;
  }
}
