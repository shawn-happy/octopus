package com.octopus.kettlex.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import org.apache.commons.lang3.StringUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = RowGeneratorConfig.class, name = "ROW_GENERATOR"),
})
public interface ReaderConfig<P extends ReaderOptions> extends StepConfig<P> {

  String getOutput();

  @Override
  default void verify() {
    StepConfig.super.verify();
    if (StringUtils.isBlank(getOutput())) {
      throw new KettleXStepConfigException(
          String.format("output cannot be null in reader %s.", getName()));
    }
  }
}
