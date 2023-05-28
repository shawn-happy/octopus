package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;

public interface Verifyable {

  /**
   * If the step config check is incorrect, throw exception.
   *
   * @throws KettleXStepConfigException
   */
  void verify();
}
