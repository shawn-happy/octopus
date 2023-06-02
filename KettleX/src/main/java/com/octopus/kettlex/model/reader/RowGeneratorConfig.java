package com.octopus.kettlex.model.reader;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.model.Field;
import com.octopus.kettlex.model.ReaderConfig;
import com.octopus.kettlex.model.ReaderOptions;
import com.octopus.kettlex.model.reader.RowGeneratorConfig.RowGeneratorOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowGeneratorConfig implements ReaderConfig<RowGeneratorOptions> {

  private String name;
  @Default private StepType type = StepType.ROW_GENERATOR;
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
      if (ArrayUtils.isEmpty(fields)) {
        throw new KettleXStepConfigException("fields cannot be empty");
      }
    }
  }
}
