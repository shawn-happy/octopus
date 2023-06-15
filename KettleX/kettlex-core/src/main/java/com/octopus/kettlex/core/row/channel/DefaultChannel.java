package com.octopus.kettlex.core.row.channel;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.management.Communication;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.record.TerminateRecord;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.Validate;

public class DefaultChannel implements Channel {

  private static final int DEFAULT_CAPACITY = 10000;

  private int capacity;
  private BlockingQueue<Record> queue;
  private ReentrantReadWriteLock lock;
  private final String id;
  private Communication currentCommunication;

  private Communication lastCommunication = new Communication();

  public DefaultChannel(String id) {
    this(DEFAULT_CAPACITY, id);
  }

  public DefaultChannel(int capacity, String id) {
    if (capacity <= 0) {
      throw new IllegalArgumentException(
          String.format("channel capacity [%d] must more than 0.", capacity));
    }
    this.capacity = capacity;
    this.queue = new ArrayBlockingQueue<>(capacity);
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void push(Record r) {
    Validate.notNull(r, "record cannot be null");
    try {
      queue.put(r);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void pushAll(Collection<Record> rs) {
    Validate.notNull(rs);
    Validate.noNullElements(rs);
    try {
      lock.writeLock().lockInterruptibly();
      this.queue.addAll(rs);
    } catch (Exception e) {
      throw new KettleXException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void pushTerminate(TerminateRecord tr) {
    push(tr);
  }

  @Override
  public Record pull() {
    try {
      return this.queue.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int size() {
    return this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.queue.isEmpty();
  }

  @Override
  public void clear() {
    this.queue.clear();
  }

  private void statePush(long recordSize) {
    currentCommunication.increaseSendRecords(recordSize);
    // 在读的时候进行统计waitCounter即可，因为写（pull）的时候可能正在阻塞，但读的时候已经能读到这个阻塞的counter数
    lastCommunication.increaseSendRecords(currentCommunication.getSendRecords());
  }
}
