package com.octopus.kettlex.core.executor;

import com.octopus.kettlex.core.statistics.Communication;
import com.octopus.kettlex.core.statistics.ExecutionState;
import com.octopus.kettlex.core.step.StepInitThread;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.steps.factory.StepFactory;
import com.octopus.kettlex.core.trans.TransMeta;
import com.octopus.kettlex.core.trans.TransStepMeta;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
@Getter
public abstract class AbstractTransExecutor implements TransExecutor {

  @Setter private String id;
  private TransMeta transMeta;
  private Communication communication;
  protected List<Step> steps;
  private AtomicBoolean status = new AtomicBoolean(false);

  public AbstractTransExecutor(TransMeta transMeta) {
    this.transMeta = transMeta;
  }

  public AbstractTransExecutor(String id, TransMeta transMeta) {
    this.id = id;
    this.transMeta = transMeta;
  }

  @Override
  public void execute(String[] args) {
    prepareExecution();
    startExecute();
  }

  protected abstract void startExecute();

  @Override
  public void stop() {
    if (CollectionUtils.isNotEmpty(steps)) {
      for (Step step : steps) {
        if (step.isRunning()) {
          step.markStatus(ExecutionState.KILLED);
          step.getStepContext().reportStepCommunication();
        }
      }
    }
    markStatus(ExecutionState.KILLED);
  }

  @Override
  public void release() {
    if (CollectionUtils.isNotEmpty(steps)) {
      for (Step step : steps) {
        step.dispose();
      }
    }
  }

  @Override
  public boolean isStopped() {
    return communication.getState().isFinished();
  }

  private void prepareExecution() {
    steps = new ArrayList<>();
    List<TransStepMeta> transStepMetas = transMeta.getSteps();
    for (TransStepMeta transStepMeta : transStepMetas) {
      StepMeta stepMeta = transStepMeta.getStepMeta();
      String name = transStepMeta.getName();
      Step step = StepFactory.createStep(stepMeta, null);
      this.steps.add(step);
    }

    StepInitThread[] initThreads = new StepInitThread[this.steps.size()];
    Thread[] threads = new Thread[this.steps.size()];
    markStatus(ExecutionState.INITIALIZING);
    // Initialize all the threads...
    for (int i = 0; i < this.steps.size(); i++) {
      final Step sid = this.steps.get(i);

      // Do the init code in the background!
      // Init all steps at once, but ALL steps need to finish before we can
      // continue properly!
      //
      initThreads[i] = new StepInitThread(sid);

      // Put it in a separate thread!
      //
      threads[i] = new Thread(initThreads[i]);
      threads[i].setName(
          "init of " + sid.getStepMeta().getStepName() + "(" + threads[i].getName() + ")");
      threads[i].start();
      sid.markStatus(ExecutionState.INITIALIZING);
    }

    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (Exception ex) {
        log.error("Error with init thread: {}", ex.getMessage());
      }
    }
    markStatus(ExecutionState.WAITNG);

    boolean ok = true;

    // All step are initialized now: see if there was one that didn't do it
    // correctly!
    //
    for (int i = 0; i < initThreads.length; i++) {
      Step step = initThreads[i].getStep();
      if (!initThreads[i].isOk()) {
        log.error("Step [{}] failed to initialize", step.getStepMeta().getStepName());
        step.markStatus(ExecutionState.FAILED);
        ok = false;
      } else {
        step.markStatus(ExecutionState.WAITNG);
      }
    }

    if (!ok) {
      // One or more steps failed on initialization.
      // Transformation is now stopped
      markStatus(ExecutionState.FAILED);
      // Halt the other threads as well, signal end-of-the line to the outside world...
      // Also explicitly call dispose() to clean up resources opened during init();
      //
      for (int i = 0; i < initThreads.length; i++) {
        Step step = initThreads[i].getStep();
        // Dispose will overwrite the status, but we set it back right after
        // this.
        step.dispose();
        if (!initThreads[i].isOk()) {
          log.error("Step [{}] failed to initialize", step.getStepMeta().getStepName());
          step.markStatus(ExecutionState.FAILED);
          ok = false;
        } else {
          step.markStatus(ExecutionState.WAITNG);
        }
      }
    }
  }

  protected void markStatus(ExecutionState status) {
    communication.setState(status);
  }
}
