package com.octopus.operators.engine.config.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.util.IdGenerator;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public abstract class AbstractStepConfig<T extends StepOptions> implements StepConfig<T> {

  private String id;
  private String name;
  private String description;
  private T options;

  @Override
  public void loadYaml(JsonNode stepNode) {
    String id = YamlUtils.getTextValue(stepNode, "id");
    this.id = StringUtils.isNotBlank(id) ? id : IdGenerator.getId();
    this.name = YamlUtils.getTextValue(stepNode, "name");
    this.description = YamlUtils.getTextValue(stepNode, "description");
    this.options = loadOptions(stepNode.path("options"));
  }

  @Override
  public List<CheckResult> check() {
    List<CheckResult> checkResults = new ArrayList<>();
    if (StringUtils.isNotBlank(this.id)) {
      checkResults.add(CheckResult.ok());
    } else {
      checkResults.add(CheckResult.error("step id is required"));
    }
    if (StringUtils.isNotBlank(this.name)) {
      checkResults.add(CheckResult.ok());
    } else {
      checkResults.add(CheckResult.error("step name is required"));
    }
    if (this.options != null) {
      checkResults.addAll(this.options.check());
    }
    return checkResults;
  }

  protected abstract T loadOptions(JsonNode options);
}
