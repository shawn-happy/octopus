package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.serialize;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.io.Serializable;

public abstract class DorisCodec implements Serializable {
  protected final String[] fieldNames;
  protected final DataWorkflowFieldType[] fieldTypes;

  protected DorisCodec(String[] fieldNames, DataWorkflowFieldType[] fieldTypes) {
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  public abstract String codec(Object[] values);
}
