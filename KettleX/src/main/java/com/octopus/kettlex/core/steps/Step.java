package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;

/**
 * @author shawn
 * @param <CONFIG>
 * @param <CONTEXT>
 */
public interface Step<CONFIG extends StepMeta> {

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
  void destory() throws KettleXException;
}
