package com.octopus.operators.kettlex.core.management;

import static com.octopus.operators.kettlex.core.management.ExecutionStatus.FAILED;
import static com.octopus.operators.kettlex.core.management.ExecutionStatus.KILLED;
import static com.octopus.operators.kettlex.core.management.ExecutionStatus.SUCCEEDED;

public class Communication {

  /** Task/Step执行状态 */
  private ExecutionStatus status;
  /** 执行报错异常 */
  private Throwable exception;
  /** 执行报告信息 */
  private String message;
  /** 执行记录的时间 */
  private long timestamp;
  /** 任务开始时间 */
  private final long startTime;
  /** 任务结束时间 */
  private long endTime;
  /** 写入到Channel中的数据量 */
  private long sendRecords;
  /** 从Channel中获取到的数据量 */
  private long receivedRecords;
  /** 参与处理的数据量 */
  private long transformRecords;
  /** 最终入库的数据量 */
  private long writerRecords;
  /** 处理失败的数据量 */
  private long errorRecords;
  /** 处理速率 单位 record/s */
  private double processSpeed;

  public Communication() {
    this.status = ExecutionStatus.SUBMITTING;
    long now = System.currentTimeMillis();
    this.startTime = now;
    this.timestamp = now;
  }

  public synchronized void increaseSendRecords(final long deltaValue) {
    this.sendRecords += deltaValue;
    updateTimestamp();
  }

  public synchronized void increaseTransformRecords(final long deltaValue) {
    this.transformRecords += deltaValue;
    updateTimestamp();
  }

  public synchronized void increaseReceivedRecords(final long deltaValue) {
    this.receivedRecords += deltaValue;
    updateTimestamp();
  }

  public synchronized void increaseWriteRecords(final long deltaValue) {
    this.writerRecords += deltaValue;
    updateTimestamp();
  }

  public synchronized void increaseErrorRecords(final long deltaValue) {
    this.errorRecords += deltaValue;
    updateTimestamp();
  }

  public synchronized void setProcessSpeed(double processSpeed) {
    this.processSpeed = processSpeed;
    updateTimestamp();
  }

  public synchronized void markStatus(ExecutionStatus status) {
    this.status = status;
    updateTimestamp();
    if (isFinished()) {
      this.endTime = System.currentTimeMillis();
    }
  }

  public synchronized void setException(Throwable exception) {
    this.exception = exception;
    updateTimestamp();
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
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

  public long getWriterRecords() {
    return writerRecords;
  }

  public long getErrorRecords() {
    return errorRecords;
  }

  public double getProcessSpeed() {
    return processSpeed;
  }

  public synchronized boolean isFinished() {
    return this.status == SUCCEEDED || this.status == FAILED || this.status == KILLED;
  }

  @Override
  public String toString() {
    return "Communication{"
        + "status="
        + status
        + ", exception="
        + exception
        + ", message='"
        + message
        + '\''
        + ", timestamp="
        + timestamp
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", sendRecords="
        + sendRecords
        + ", receivedRecords="
        + receivedRecords
        + ", transformRecords="
        + transformRecords
        + ", writerRecords="
        + writerRecords
        + ", errorRecords="
        + errorRecords
        + '}';
  }

  public synchronized void mergeFrom(final Communication otherComm) {
    if (otherComm == null) {
      return;
    }

    long receivedRecords = otherComm.getReceivedRecords();
    long sendRecords = otherComm.getSendRecords();
    long transformRecords = otherComm.getTransformRecords();
    this.receivedRecords += receivedRecords;
    this.sendRecords += sendRecords;
    this.transformRecords += transformRecords;
    this.writerRecords += otherComm.getWriterRecords();
    this.errorRecords += otherComm.getErrorRecords();

    // 合并state
    mergeStateFrom(otherComm);

    this.exception = this.exception == null ? otherComm.getException() : this.exception;
    this.message = this.message == null ? otherComm.getMessage() : this.message;
    this.updateTimestamp();
  }

  public synchronized void mergeStateFrom(final Communication otherComm) {
    ExecutionStatus executionStatus = this.getStatus();
    if (otherComm == null) {
      return;
    }

    if (this.status == ExecutionStatus.FAILED
        || otherComm.getStatus() == ExecutionStatus.FAILED
        || this.status == ExecutionStatus.KILLED
        || otherComm.getStatus() == ExecutionStatus.KILLED) {
      executionStatus = ExecutionStatus.FAILED;
    } else if (this.status.isRunning() || otherComm.getStatus().isRunning()) {
      executionStatus = ExecutionStatus.RUNNING;
    }

    this.markStatus(executionStatus);
  }

  private void updateTimestamp() {
    this.timestamp = System.currentTimeMillis();
  }
}
