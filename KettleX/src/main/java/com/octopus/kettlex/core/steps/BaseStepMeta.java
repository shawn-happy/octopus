package com.octopus.kettlex.core.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.utils.JsonUtil;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class BaseStepMeta implements StepMeta {

  private String stepName;
  private boolean enableAuditLog = false;

  public BaseStepMeta() {}

  public BaseStepMeta(String json) {
    loadJson(json);
  }

  /**
   * Load the values for this step from an JSON Node
   *
   * @param stepNode the Node to get the info from
   * @throws KettleXJSONException When an unexpected JSON error occurred
   */
  protected void loadJson(JsonNode stepNode) throws KettleXJSONException {}

  private void loadJson(String json) {
    loadJson(
        JsonUtil.toJsonNode(json)
            .orElseThrow(() -> new KettleXJSONException("an unexpected JSON error occurred")));
  }
}
