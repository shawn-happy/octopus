package com.octopus.operators.kettlex.core.steps.config;

import com.octopus.operators.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.operators.kettlex.core.steps.config.TransformerConfig.TransformerOptions;
import org.apache.commons.lang3.StringUtils;

public interface TransformerConfig<P extends TransformerOptions> extends StepConfig<P> {

  /**
   * 目前只支持单个input，如果是多个input，会出现以下问题：
   * <li>1. 多个input的字段（名称，类型，个数等）可能不一样。
   * <li>2. 多个input的数据流速不一样，如果需要对多个Input进行处理，可能会造成数据丢失。
   * <li>3. 多个input的处理过程可能不一样。
   */
  String getInput();

  String getOutput();

  @Override
  default void verify() {
    StepConfig.super.verify();
    if (StringUtils.isBlank(getInput())) {
      throw new KettleXStepConfigException(
          String.format("input cannot be null in transformation %s.", getName()));
    }
    if (StringUtils.isBlank(getOutput())) {
      throw new KettleXStepConfigException(
          String.format("output cannot be null in transformation %s.", getName()));
    }
  }

  interface TransformerOptions extends StepOptions {}
}
