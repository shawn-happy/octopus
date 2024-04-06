package io.github.shawn.octopus.fluxus.engine.connector.source.file;

import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.serialization.DeserializationSchema;
import java.io.IOException;

public class FileSourceConverter implements RowRecordConverter<byte[]> {

  private DeserializationSchema<RowRecord> deserialization;

  public void setDeserialization(DeserializationSchema<RowRecord> deserialization) {
    this.deserialization = deserialization;
  }

  @Override
  public RowRecord convert(byte[] message) {
    try {
      return deserialization.deserialize(message);
    } catch (IOException e) {
      throw new DataWorkflowException(e);
    }
  }
}
