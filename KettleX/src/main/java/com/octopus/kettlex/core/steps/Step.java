package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.statistics.ExecutionState;

public interface Step {

  StepMeta getStepMeta();

  StepContext getStepContext();

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param sm The metadata to work with
   * @param sc The data to initialize
   */
  boolean init();

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
  boolean processRow() throws KettleXException;

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sc The data to dispose of
   */
  void dispose();

  Integer getLevelSeq();

  /**
   * Put a row on the destination rowsets.
   *
   * @param row The row to send to the destinations steps
   * @throws KettleXException
   */
  public void putRow(Record row, Object[] data) throws KettleXException;

  /**
   * @return a row from the source step(s).
   * @throws KettleXException
   */
  public Object[] getRow() throws KettleXException;

  /** Signal output done to destination steps */
  public void setOutputDone();

  void markStatus(ExecutionState status);

  /** Flags all rowsets as stopped/completed/finished. */
  void stopAll();

  /** @return true if the step is running after having been initialized */
  boolean isRunning();

  /** @return True if the step is marked as stopped. Execution should stop immediate. */
  boolean isStopped();

  /**
   * Sets the number of errors
   *
   * @param errors the number of errors to set
   */
  void setErrors(long errors);
}
