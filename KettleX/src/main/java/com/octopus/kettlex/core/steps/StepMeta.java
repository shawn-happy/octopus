package com.octopus.kettlex.core.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleJSONException;
import com.octopus.kettlex.core.exception.KettleXException;

public interface StepMeta {

  /** Set default values */
  void setDefault();

  /**
   * Get the JSON that represents the values in this step
   *
   * @return the JSON that represents the metadata in this step
   * @throws KettleXException in case there is a conversion or JSON encoding error
   */
  String getJSON() throws KettleXException;

  /**
   * Load the values for this step from an XML Node
   *
   * @param stepNode the Node to get the info from
   * @throws KettleJSONException When an unexpected XML error occurred. (malformed etc.)
   */
  void loadJson(JsonNode stepNode) throws KettleJSONException;

  /**
   * Get a new instance of the appropriate data class. This data class implements the
   * StepDataInterface. It basically contains the persisting data that needs to live on, even if a
   * worker thread is terminated.
   *
   * @return The appropriate StepDataInterface class.
   */
  StepContext getStepContext();
}
