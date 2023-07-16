package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.context.StepContext;
import com.octopus.operators.kettlex.core.exception.KettleXException;
import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.listener.StepListener;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.channel.Channel;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import com.octopus.operators.kettlex.core.steps.config.StepConfig;
import com.octopus.operators.kettlex.core.steps.config.StepConfig.StepOptions;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;
import com.octopus.operators.kettlex.core.utils.JsonUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseStep<C extends StepConfig<?>> implements Step<C> {

  @Getter private C stepConfig;
  private List<Channel> outputChannels;
  private Channel inputChannel;
  private volatile boolean shutdown;
  private final ReentrantReadWriteLock inputChannelLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock outputChannelLock = new ReentrantReadWriteLock();
  protected List<StepListener> stepListeners = new ArrayList<>();
  protected StepContext stepContext;

  protected BaseStep() {}

  @Override
  public boolean init(StepConfigChannelCombination<C> combination) throws KettleXException {
    Collections.sort(stepListeners);
    this.stepConfig = combination.getStepConfig();
    this.outputChannels = combination.getOutputChannels();
    this.inputChannel = combination.getInputChannel();
    this.stepContext = combination.getStepContext();
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
    stepListeners.forEach(stepListener -> stepListener.onPrepare(this.stepContext));
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
        this.stepContext.getCommunication().increaseSendRecords(1);
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
        this.stepContext.getCommunication().increaseReceivedRecords(1);
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

  @Override
  public void addStepListeners(StepListener stepListener) {
    stepListeners.add(stepListener);
  }

  protected boolean isShutdown() {
    return shutdown;
  }

  protected void setError(Exception e) {
    stepListeners.forEach(stepListener -> stepListener.onError(stepContext, e));
    log.error("{} execute error. {}", stepConfig.getName(), e.getMessage(), e);
    throw new KettleXStepExecuteException(e);
  }
}
