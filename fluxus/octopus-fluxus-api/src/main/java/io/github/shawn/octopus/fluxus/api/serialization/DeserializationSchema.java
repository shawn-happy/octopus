package io.github.shawn.octopus.fluxus.api.serialization;

import java.io.IOException;

public interface DeserializationSchema<T> {
  /**
   * Deserializes the byte message.
   *
   * @param message The message, as a byte array.
   * @return The deserialized message as an SeaTunnel Row (null if the message cannot be
   *     deserialized).
   */
  T deserialize(byte[] message) throws IOException;
}
