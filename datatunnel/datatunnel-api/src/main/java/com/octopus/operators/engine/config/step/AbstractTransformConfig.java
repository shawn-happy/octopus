package com.octopus.operators.engine.config.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.List;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
public abstract class AbstractTransformConfig<T extends StepOptions> extends AbstractStepConfig<T>
    implements TransformConfig<T> {
  private List<String> sourceTables;
  private String resultTable;

  @Override
  public void loadYaml(JsonNode stepNode) {
    super.loadYaml(stepNode);
    this.sourceTables = YamlUtils.getStringArray(stepNode, "sourceTables");
    this.resultTable = YamlUtils.getTextValue(stepNode, "resultTable");
  }

  @Override
  public List<CheckResult> check() {
    List<CheckResult> checkResults = super.check();
    if (CollectionUtils.isNotEmpty(sourceTables)) {
      checkResults.add(CheckResult.ok());
    } else {
      checkResults.add(CheckResult.error("sourceTables is required in transformConfig"));
    }
    if (StringUtils.isNotBlank(this.resultTable)) {
      checkResults.add(CheckResult.ok());
    } else {
      checkResults.add(CheckResult.error("resultTable is required in transformConfig"));
    }
    return checkResults;
  }
}
