package com.octopus.kettlex.core.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXJSONException;

public interface StepMeta {

  public StepType getStepType();

  public String getStepName();

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
   * @throws KettleXJSONException When an unexpected XML error occurred. (malformed etc.)
   */
  void loadJson(JsonNode stepNode) throws KettleXJSONException;
}
