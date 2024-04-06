package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write;

import java.io.IOException;
import java.io.InputStream;
import org.jetbrains.annotations.NotNull;

public class RecordStream extends InputStream {
  private final RecordBuffer recordBuffer;

  @Override
  public int read() throws IOException {
    return 0;
  }

  public RecordStream(int bufferSize, int bufferCount) {
    this.recordBuffer = new RecordBuffer(bufferSize, bufferCount);
  }

  public void startInput() {
    recordBuffer.startBufferData();
  }

  public void endInput() throws IOException {
    recordBuffer.stopBufferData();
  }

  @Override
  public int read(@NotNull byte[] buff) throws IOException {
    try {
      return recordBuffer.read(buff);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void write(byte[] buff) throws IOException {
    try {
      recordBuffer.write(buff);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
