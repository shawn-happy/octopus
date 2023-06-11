package com.octopus.kettlex.reader.rowgenerator;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.config.ReaderConfig;
import com.octopus.kettlex.reader.rowgenerator.RowGeneratorConfig.RowGeneratorOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowGeneratorConfig implements ReaderConfig<RowGeneratorOptions> {

  private String id;
  private String name;
  private RowGeneratorOptions options;
  private String output;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RowGeneratorOptions implements ReaderOptions {
    private Integer rowLimit;
    private Field[] fields;

    @Override
    public void verify() {
      if (rowLimit != null && rowLimit < 0) {
        throw new KettleXStepConfigException("row limit cannot be less than 0");
      }
      if (ArrayUtils.isEmpty(fields)) {
        throw new KettleXStepConfigException("fields cannot be empty");
      }
    }
  }
}
