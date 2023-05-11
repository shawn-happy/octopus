package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;

public interface Step<SM extends StepMeta, SC extends StepContext> {

  /**
   * Perform the equivalent of processing one row. Typically this means reading a row from input
   * (getRow()) and passing a row to output (putRow)).
   *
   * @param sm The steps metadata to work with
   * @param sc The steps temporary working data to work with (database connections, result sets,
   *     caches, temporary variables, etc.)
   * @return false if no more rows can be processed or an error occurred.
   * @throws KettleXException
   */
  boolean processRow(SM sm, SC sc) throws KettleXException;

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param sm The metadata to work with
   * @param sc The data to initialize
   */
  boolean init(SM sm, SC sc);

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sc The data to dispose of
   */
  void dispose(SM smi, SC sc);

  /** Mark the start time of the step. */
  void markStart();

  /** Mark the end time of the step. */
  void markSuccess();

  void markFailed();

  /**
   * Stop running operations...
   *
   * @param sm The metadata that might be needed by the step to stop running.
   * @param sc The interface to the step data containing the connections, resultsets, open files,
   *     etc.
   * @throws KettleXException
   */
  void stopRunning(SM sm, SC sc) throws KettleXException;

  /**
   * @return true if the step is running after having been initialized
   */
  boolean isRunning();

  /**
   * Flag the step as running or not
   *
   * @param running the running flag to set
   */
  void setRunning(boolean running);

  /**
   * @return True if the step is marked as stopped. Execution should stop immediate.
   */
  boolean isStopped();

  /**
   * @param stopped true if the step needs to be stopped
   */
  void setStopped(boolean stopped);

  /**
   * @return True if the step is paused
   */
  boolean isPaused();

  /** Flags all rowsets as stopped/completed/finished. */
  void stopAll();

  /** Pause a running step */
  void pauseRunning();

  /** Resume a running step */
  void resumeRunning();

  /**
   * Get the name of the step.
   *
   * @return the name of the step
   */
  String getStepName();
}
