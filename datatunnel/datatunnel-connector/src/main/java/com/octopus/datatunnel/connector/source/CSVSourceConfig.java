package com.octopus.datatunnel.connector.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.datatunnel.connector.source.CSVSourceConfig.CSVSourceOptions;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.step.AbstractSourceConfig;
import com.octopus.operators.engine.config.step.StepOptions;
import com.octopus.operators.engine.table.catalog.Column;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CSVSourceConfig extends AbstractSourceConfig<CSVSourceOptions> {

  @Default private final String type = "csv";
  private CSVSourceOptions options;

  @Override
  protected CSVSourceOptions loadOptions(JsonNode options) {
    return null;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSourceOptions implements StepOptions {

    private String[] paths;
    private List<Column> schemas;
    private String encoding;
    @Default private boolean header = true;
    @Default private boolean ignoreError = false;
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    @Override
    public StepOptions loadYaml(String yaml) {
      return null;
    }

    @Override
    public List<CheckResult> check() {
      return null;
    }
  }
}
