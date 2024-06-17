package com.octopus.operators.engine.config.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public abstract class AbstractSinkConfig<T extends StepOptions> extends AbstractStepConfig<T>
    implements SinkConfig<T> {

  private String sourceTable;
  private WriteMode writeMode;

  @Override
  public void loadYaml(JsonNode stepNode) {
    super.loadYaml(stepNode);
    this.sourceTable = YamlUtils.getTextValue(stepNode, "sourceTable");
    this.writeMode = WriteMode.of(YamlUtils.getTextValue(stepNode, "writeMode"));
    if (this.writeMode == null) {
      this.writeMode = WriteMode.APPEND;
    }
  }

  @Override
  public List<CheckResult> check() {
    List<CheckResult> checkResults = super.check();
    if (StringUtils.isNotBlank(sourceTable)) {
      checkResults.add(CheckResult.ok());
    } else {
      checkResults.add(CheckResult.error("sourceTable is required in sinkConfig"));
    }
    return checkResults;
  }
}
