package com.octopus.kettlex.core.management;

import static com.octopus.kettlex.core.management.ExecutionStatus.FAILED;
import static com.octopus.kettlex.core.management.ExecutionStatus.KILLED;
import static com.octopus.kettlex.core.management.ExecutionStatus.SUCCEEDED;

public class Communication {

  /** Task/Step执行状态 */
  private ExecutionStatus status;
  /** 执行报错异常 */
  private Throwable exception;
  /** 执行报告信息 */
  private String message;
  /** 执行记录的时间 */
  private long timestamp;
  /** 读取到的数据量 */
  private long sendRecords;
  /** 转换的数据量 */
  private long transformRecords;
  /** 接受到的数据量 */
  private long receivedRecords;

  public synchronized void increaseSendRecords(final long deltaValue) {
    this.sendRecords += deltaValue;
  }

  public synchronized void increaseTransformRecords(final long deltaValue) {
    this.transformRecords += deltaValue;
  }

  public synchronized void increaseReceivedRecords(final long deltaValue) {
    this.receivedRecords += deltaValue;
  }

  public synchronized void markStatus(ExecutionStatus status) {
    this.status = status;
  }

  public synchronized void setException(Throwable exception) {
    this.exception = exception;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public synchronized String getThrowableMessage() {
    return this.exception == null ? "" : this.exception.getMessage();
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  public Throwable getException() {
    return exception;
  }

  public String getMessage() {
    return message;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getSendRecords() {
    return sendRecords;
  }

  public long getTransformRecords() {
    return transformRecords;
  }

  public long getReceivedRecords() {
    return receivedRecords;
  }

  public synchronized boolean isFinished() {
    return this.status == SUCCEEDED || this.status == FAILED || this.status == KILLED;
  }
}
