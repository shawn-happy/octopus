package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;

/** Channel of record stream and HTTP data stream. */
@Slf4j
public class RecordBuffer {
  BlockingQueue<ByteBuffer> writeQueue;
  BlockingQueue<ByteBuffer> readQueue;
  int bufferCapacity;
  int queueSize;
  ByteBuffer currentWriteBuffer;
  ByteBuffer currentReadBuffer;

  public RecordBuffer(int capacity, int queueSize) {
    log.info("init RecordBuffer capacity {}, count {}", capacity, queueSize);
    checkState(capacity > 0);
    checkState(queueSize > 1);
    this.writeQueue = new ArrayBlockingQueue<>(queueSize);
    for (int index = 0; index < queueSize; index++) {
      this.writeQueue.add(ByteBuffer.allocate(capacity));
    }
    readQueue = new LinkedBlockingDeque<>();
    this.bufferCapacity = capacity;
    this.queueSize = queueSize;
  }

  public void startBufferData() {
    log.info(
        "start buffer data, read queue size {}, write queue size {}",
        readQueue.size(),
        writeQueue.size());
    checkState(readQueue.isEmpty());
    checkState(writeQueue.size() == queueSize);
    for (ByteBuffer byteBuffer : writeQueue) {
      checkState(byteBuffer.position() == 0);
      checkState(byteBuffer.remaining() == bufferCapacity);
    }
  }

  public void stopBufferData() throws IOException {
    try {
      // add Empty buffer as finish flag.
      boolean isEmpty = false;
      if (currentWriteBuffer != null) {
        currentWriteBuffer.flip();
        // check if the current write buffer is empty.
        isEmpty = currentWriteBuffer.limit() == 0;
        readQueue.put(currentWriteBuffer);
        currentWriteBuffer = null;
      }
      if (!isEmpty) {
        ByteBuffer byteBuffer = writeQueue.take();
        byteBuffer.flip();
        checkState(byteBuffer.limit() == 0);
        readQueue.put(byteBuffer);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void write(byte[] buf) throws InterruptedException {
    int wPos = 0;
    do {
      if (currentWriteBuffer == null) {
        currentWriteBuffer = writeQueue.take();
      }
      int available = currentWriteBuffer.remaining();
      int nWrite = Math.min(available, buf.length - wPos);
      currentWriteBuffer.put(buf, wPos, nWrite);
      wPos += nWrite;
      if (currentWriteBuffer.remaining() == 0) {
        currentWriteBuffer.flip();
        readQueue.put(currentWriteBuffer);
        currentWriteBuffer = null;
      }
    } while (wPos != buf.length);
  }

  public int read(byte[] buf) throws InterruptedException {
    if (currentReadBuffer == null) {
      currentReadBuffer = readQueue.take();
    }
    // add empty buffer as end flag
    if (currentReadBuffer.limit() == 0) {
      recycleBuffer(currentReadBuffer);
      currentReadBuffer = null;
      checkState(readQueue.isEmpty());
      return -1;
    }
    int available = currentReadBuffer.remaining();
    int nRead = Math.min(available, buf.length);
    currentReadBuffer.get(buf, 0, nRead);
    if (currentReadBuffer.remaining() == 0) {
      recycleBuffer(currentReadBuffer);
      currentReadBuffer = null;
    }
    return nRead;
  }

  private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
    buffer.clear();
    writeQueue.put(buffer);
  }
}
