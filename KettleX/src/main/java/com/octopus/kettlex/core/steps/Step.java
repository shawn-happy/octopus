package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.model.StepConfig;

/**
 * @author shawn
 * @param <CONFIG>
 */
public interface Step<CONFIG extends StepConfig<?>> {

  CONFIG getStepConfig();

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @throws KettleXException
   */
  boolean init() throws KettleXException;

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @throws KettleXException
   */
  void destroy() throws KettleXException;

  void shutdown() throws KettleXException;
}
