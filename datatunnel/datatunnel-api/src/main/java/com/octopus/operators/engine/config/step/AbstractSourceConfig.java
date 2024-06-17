package com.octopus.operators.engine.config.step;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.table.catalog.Column;
import com.octopus.operators.engine.util.YamlUtils;
import java.util.List;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
public abstract class AbstractSourceConfig<T extends StepOptions> extends AbstractStepConfig<T>
    implements SourceConfig<T> {

  private String resultTable;
  private List<Column> columns;

  @Override
  public void loadYaml(JsonNode stepNode) {
    super.loadYaml(stepNode);
    this.resultTable = YamlUtils.getTextValue(stepNode, "resultTable");
    JsonNode columnsNode = stepNode.path("columns");
    if (YamlUtils.isNotBlank(columnsNode)) {
      this.columns = YamlUtils.fromYaml(columnsNode, new TypeReference<List<Column>>() {});
    }
  }

  @Override
  public List<CheckResult> check() {
    List<CheckResult> checkResults = super.check();
    if (StringUtils.isBlank(this.resultTable)) {
      checkResults.add(CheckResult.error("resultTable is required"));
    } else {
      checkResults.add(CheckResult.ok());
    }
    if (CollectionUtils.isNotEmpty(this.columns)) {
      checkResults.add(CheckResult.ok());
    } else {
      checkResults.add(CheckResult.error("columns is required"));
    }
    return checkResults;
  }
}
