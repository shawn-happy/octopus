package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXException;
import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.channel.Channel;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import com.octopus.operators.kettlex.core.steps.config.StepConfig;
import com.octopus.operators.kettlex.core.steps.config.StepConfig.StepOptions;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import com.octopus.operators.kettlex.core.utils.JsonUtil;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseStep<C extends StepConfig<?>> implements Step<C> {

  @Getter private C stepConfig;
  private List<Channel> outputChannels;
  private Channel inputChannel;
  private Communication communication;
  private volatile boolean shutdown;
  private final ReentrantReadWriteLock inputChannelLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock outputChannelLock = new ReentrantReadWriteLock();

  protected BaseStep() {}

  @Override
  public boolean init(StepConfigChannelCombination<C> combination) throws KettleXException {
    this.stepConfig = combination.getStepConfig();
    this.outputChannels = combination.getOutputChannels();
    this.inputChannel = combination.getInputChannel();
    this.communication = combination.getCommunication();
    if (shutdown) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    if (log.isDebugEnabled()) {
      log.debug("init step config. {}", JsonUtil.toJson(stepConfig));
    }
    StepOptions options = stepConfig.getOptions();
    // 有些组件没有options，可能会为Null
    if (options != null) {
      options.verify();
    }
    doInit(stepConfig);
    communication.markStatus(ExecutionStatus.WAITING);
    return true;
  }

  @Override
  public void destroy() throws KettleXException {
    log.info("destroy step. {}", stepConfig.getName());
    doDestroy();
  }

  protected void doDestroy() throws KettleXException {}

  protected void doInit(C stepConfig) throws KettleXException {}

  protected void putRow(Record record) {
    if (shutdown) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    outputChannelLock.readLock().lock();
    try {
      for (Channel outputChannel : outputChannels) {
        outputChannel.push(record);
      }
      if (!(record instanceof TerminateRecord)) {
        communication.increaseSendRecords(1);
      }
    } finally {
      outputChannelLock.readLock().unlock();
    }
  }

  protected Record getRow() {
    if (shutdown) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    inputChannelLock.readLock().lock();
    try {
      Record record = inputChannel.pull();
      if (!(record instanceof TerminateRecord)) {
        communication.increaseReceivedRecords(1);
      }
      return record;
    } finally {
      inputChannelLock.readLock().unlock();
    }
  }

  @Override
  public void shutdown() throws KettleXException {
    shutdown = true;
  }

  protected boolean isShutdown() {
    return shutdown;
  }

  protected Communication getCommunication() {
    return communication;
  }

  protected void setError(Exception e) {
    communication.markStatus(ExecutionStatus.FAILED);
    communication.setException(e);
    log.error("{} execute error. {}", stepConfig.getName(), e.getMessage(), e);
    throw new KettleXStepExecuteException(e);
  }
}
