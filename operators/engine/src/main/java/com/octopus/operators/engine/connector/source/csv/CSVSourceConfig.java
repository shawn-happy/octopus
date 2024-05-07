package com.octopus.operators.engine.connector.source.csv;

import com.octopus.operators.engine.config.source.SourceConfig;
import com.octopus.operators.engine.config.source.SourceOptions;
import com.octopus.operators.engine.config.source.SourceType;
import com.octopus.operators.engine.connector.source.csv.CSVSourceConfig.CSVSourceOptions;
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
public class CSVSourceConfig implements SourceConfig<CSVSourceOptions> {

  private final SourceType type = SourceType.CSV;
  private String name;
  private CSVSourceOptions options;
  private String output;
  private Integer parallelism;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSourceOptions implements SourceOptions {

    private String[] paths;
    private List<Column> schemas;
    private String encoding;
    @Default private boolean header = true;
    @Default private boolean ignoreError = false;
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    @Override
    public SourceOptions toOptions(String json) {
      return null;
    }

    @Override
    public String toJson() {
      return null;
    }
  }
}
