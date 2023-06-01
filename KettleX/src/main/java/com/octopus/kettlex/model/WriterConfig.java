package com.octopus.kettlex.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import org.apache.commons.lang3.StringUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = LogMessageConfig.class, name = "LOG_MESSAGE"),
})
public interface WriterConfig<P extends WriterOptions> extends StepConfig<P> {

  String getInput();

  WriteMode getWriteMode();

  @Override
  default void verify() {
    StepConfig.super.verify();
    if (StringUtils.isBlank(getInput())) {
      throw new KettleXStepConfigException(
          String.format("input cannot be null in writer %s.", getName()));
    }
  }
}
