package com.octopus.kettlex.model;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.core.steps.Verifyable;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * step config
 *
 * @param <P>
 */
public interface StepConfig<P extends Options> extends Verifyable {

  String getName();

  StepType getType();

  P getOptions();

  @Override
  default void verify() {
    if (StringUtils.isBlank(getName())) {
      throw new KettleXStepConfigException("Step name cannot be null");
    }
    if (Objects.isNull(getType())) {
      throw new KettleXStepConfigException("Step type cannot be null");
    }
    if (!Objects.isNull(getOptions())) {
      getOptions().verify();
    }
  }
}
