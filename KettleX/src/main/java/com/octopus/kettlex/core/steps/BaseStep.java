package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.channel.Channel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.Options;
import com.octopus.kettlex.model.StepConfig;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseStep<C extends StepConfig<?>> implements Step<C> {

  protected final C stepConfig;
  private List<Channel> outputChannels;
  private Channel inputChannel;
  private final ReentrantReadWriteLock inputChannelLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock outputChannelLock = new ReentrantReadWriteLock();

  protected BaseStep(C stepConfig) {
    this.stepConfig = stepConfig;
  }

  protected void setOutputChannels(List<Channel> outputChannels) {
    this.outputChannels = outputChannels;
  }

  protected void setInputChannel(Channel inputChannel) {
    this.inputChannel = inputChannel;
  }

  @Override
  public boolean init() throws KettleXException {
    if (log.isDebugEnabled()) {
      log.debug("init step config. {}", JsonUtil.toJson(stepConfig));
    }
    Options options = stepConfig.getOptions();
    // 有些组件没有options，可能会为Null
    if (options != null) {
      options.verify();
    }
    doInit();
    return true;
  }

  @Override
  public void destroy() throws KettleXException {
    log.info("destroy step. {}", stepConfig.getName());
    doDestroy();
  }

  protected void doDestroy() throws KettleXException {}

  protected void doInit() throws KettleXException {}

  protected void putRow(Record record) {
    if (stepConfig.getType().getPrimaryCategory() == StepType.PrimaryCategory.SINK) {
      throw new KettleXStepExecuteException("sink operator has no output");
    }
    outputChannelLock.readLock().lock();
    try {
      for (Channel outputChannel : outputChannels) {
        outputChannel.push(record);
      }
    } finally {
      outputChannelLock.readLock().unlock();
    }
  }

  protected Record getRow() {
    if (stepConfig.getType().getPrimaryCategory() == StepType.PrimaryCategory.SOURCE) {
      throw new KettleXStepExecuteException("source operator has no input");
    }
    inputChannelLock.readLock().lock();
    try {
      return inputChannel.pull();
    } finally {
      inputChannelLock.readLock().unlock();
    }
  }
}
