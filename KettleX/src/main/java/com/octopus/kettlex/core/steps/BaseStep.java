package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.channel.Channel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.model.Options;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.runtime.TaskCombination;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public abstract class BaseStep<C extends StepConfig<?>> implements Step<C> {

  protected final C stepConfig;
  private final TaskCombination taskCombination;
  private List<Channel> outputChannels;
  private Channel inputChannels;
  private final ReentrantReadWriteLock inputChannelLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock outputChannelLock = new ReentrantReadWriteLock();

  protected BaseStep(C stepConfig, TaskCombination taskCombination) {
    this.stepConfig = stepConfig;
    this.taskCombination = taskCombination;
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
    initChannel();
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

  private void initChannel() {
    log.info("step {} start to init data channel.", stepConfig.getName());
    StepConfig<?> parentStep = taskCombination.findParentStep(stepConfig.getName());
    List<StepConfig<?>> childSteps = taskCombination.findChildSteps(stepConfig.getName());
    inputChannelLock.writeLock().lock();
    outputChannelLock.writeLock().lock();
    try {
      if (parentStep != null) {
        inputChannels =
            taskCombination.getStepLink(parentStep.getName(), stepConfig.getName()).getChannel();
      }
      if (CollectionUtils.isNotEmpty(childSteps)) {
        outputChannels =
            childSteps.stream()
                .map(
                    sc ->
                        taskCombination
                            .getStepLink(stepConfig.getName(), sc.getName())
                            .getChannel())
                .collect(Collectors.toList());
      }
    } finally {
      inputChannelLock.writeLock().unlock();
      outputChannelLock.writeLock().unlock();
    }
  }

  protected void putRow(Record record) {
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
    inputChannelLock.readLock().lock();
    try {
      return inputChannels.pull();
    } finally {
      inputChannelLock.readLock().unlock();
    }
  }
}
