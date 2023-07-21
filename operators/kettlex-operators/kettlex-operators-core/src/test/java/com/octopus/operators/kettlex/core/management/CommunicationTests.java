package com.octopus.operators.kettlex.core.management;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CommunicationTests {

  private static Communication communication;

  @BeforeAll
  public static void setup() throws Exception {
    communication = new Communication();
    communication.markStatus(ExecutionStatus.RUNNING);
    Thread[] sendThreads = new Thread[100];
    for (int i = 0; i < 100; i++) {
      sendThreads[i] = new Thread(() -> communication.increaseSendRecords(100));
      sendThreads[i].start();
    }

    Thread[] receiveThreads = new Thread[100];
    for (int i = 0; i < 100; i++) {
      receiveThreads[i] = new Thread(() -> communication.increaseReceivedRecords(100));
      receiveThreads[i].start();
    }

    Thread[] writeThreads = new Thread[100];
    for (int i = 0; i < 100; i++) {
      writeThreads[i] = new Thread(() -> communication.increaseWriteRecords(30));
      writeThreads[i].start();
    }

    Thread[] transferThreads = new Thread[100];
    for (int i = 0; i < 100; i++) {
      transferThreads[i] = new Thread(() -> communication.increaseTransformRecords(50));
      transferThreads[i].start();
    }

    for (Thread sendThread : sendThreads) {
      sendThread.join();
    }

    for (Thread receiveThread : receiveThreads) {
      receiveThread.join();
    }

    for (Thread writeThread : writeThreads) {
      writeThread.join();
    }

    for (Thread transferThread : transferThreads) {
      transferThread.join();
    }
  }

  @Test
  public void init() {
    Communication init = new Communication();
    ExecutionStatus status = init.getStatus();
    assertEquals(ExecutionStatus.SUBMITTING, status);
  }

  @Test
  public void increase() {
    assertEquals(100 * 100, communication.getSendRecords());
    assertEquals(100 * 100, communication.getReceivedRecords());
    assertEquals(100 * 30, communication.getWriterRecords());
    assertEquals(100 * 50, communication.getTransformRecords());
  }

  @Test
  public void mergeFrom() {
    Communication communication = new Communication();
    communication.increaseSendRecords(1000);
    communication.increaseReceivedRecords(1000);
    communication.increaseTransformRecords(800);
    communication.increaseWriteRecords(700);
    communication.markStatus(ExecutionStatus.RUNNING);

    CommunicationTests.communication.mergeFrom(communication);
    assertEquals(100 * 100 + 1000, CommunicationTests.communication.getSendRecords());
    assertEquals(100 * 100 + 1000, CommunicationTests.communication.getReceivedRecords());
    assertEquals(100 * 30 + 700, CommunicationTests.communication.getWriterRecords());
    assertEquals(100 * 50 + 800, CommunicationTests.communication.getTransformRecords());
    assertEquals(ExecutionStatus.RUNNING, CommunicationTests.communication.getStatus());

    communication.markStatus(ExecutionStatus.SUCCEEDED);
    CommunicationTests.communication.mergeFrom(communication);
    assertEquals(ExecutionStatus.RUNNING, CommunicationTests.communication.getStatus());

    communication.markStatus(ExecutionStatus.FAILED);
    CommunicationTests.communication.mergeFrom(communication);
    assertEquals(ExecutionStatus.FAILED, CommunicationTests.communication.getStatus());
  }
}
