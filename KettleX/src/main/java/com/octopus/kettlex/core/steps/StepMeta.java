package com.octopus.kettlex.core.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.read.rowgenerator.RowGeneratorMeta;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "stepType")
@JsonSubTypes({@JsonSubTypes.Type(value = RowGeneratorMeta.class, name = "ROW_GENERATOR_INPUT")})
public interface StepMeta {

  public StepType getStepType();

  public String getStepName();

  /**
   * Get the JSON that represents the values in this step
   *
   * @return the JSON that represents the metadata in this step
   * @throws KettleXException in case there is a conversion or JSON encoding error
   */
  String getJSON() throws KettleXException;
}
