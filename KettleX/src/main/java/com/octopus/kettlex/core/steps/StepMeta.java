package com.octopus.kettlex.core.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGeneratorMeta;
import com.octopus.kettlex.core.steps.transform.valueMapper.ValueMapperMeta;
import com.octopus.kettlex.core.steps.writer.log.LogMessageMeta;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * step config
 *
 * @author shawn
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "stepType")
@JsonSubTypes({
  @JsonSubTypes.Type(value = RowGeneratorMeta.class, name = "ROW_GENERATOR"),
  @JsonSubTypes.Type(value = ValueMapperMeta.class, name = "VALUE_MAPPER"),
  @JsonSubTypes.Type(value = LogMessageMeta.class, name = "LOG_MESSAGE"),
})
public interface StepMeta extends Verifyable {

  String getId();

  String getName();

  StepType getStepType();

  @Override
  default void verify() {
    if (Objects.isNull(getStepType())) {
      throw new KettleXStepConfigException("stepType can not be null");
    }
    if (StringUtils.isBlank(getName())) {
      throw new KettleXStepConfigException("step name can not be null");
    }
    if (StringUtils.isBlank(getId())) {
      throw new KettleXStepConfigException("step id can not be null");
    }
  }
}
