package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.config.StepConfig;
import com.octopus.kettlex.core.steps.config.StepConfig.StepOptions;
import com.octopus.kettlex.core.steps.config.StepConfigChannelCombination;

/**
 * @author shawn
 * @param <CONFIG>
 */
public interface Step<CONFIG extends StepConfig<? extends StepOptions>> {

  CONFIG getStepConfig();

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @throws KettleXException
   */
  boolean init(StepConfigChannelCombination<CONFIG> combination) throws KettleXException;

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @throws KettleXException
   */
  void destroy() throws KettleXException;

  void shutdown() throws KettleXException;
}
