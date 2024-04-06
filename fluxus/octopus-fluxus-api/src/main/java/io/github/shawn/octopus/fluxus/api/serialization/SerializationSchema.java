package io.github.shawn.octopus.fluxus.api.serialization;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;

public interface SerializationSchema {
  /**
   * Serializes the incoming element to a specified type.
   *
   * @param record The incoming element to be serialized
   * @return The serialized element.
   */
  byte[] serialize(RowRecord record);
}
