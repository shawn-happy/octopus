package io.github.shawn.octopus.fluxus.engine.pipeline.context;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.pipeline.ExecutionPlan;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.PipelineStatus;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricSummary;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobContext {
  @Getter private final JobConfig jobConfig;
  private final AtomicReference<PipelineStatus> status =
      new AtomicReference<>(PipelineStatus.CREATING);
  @Getter private final PipelineMetricSummary pipelineMetricSummary;
  private long startTime;
  private long endTime;
  @Setter @Getter private Throwable throwable;

  public JobContext(ExecutionPlan executionPlan) {
    this.jobConfig = executionPlan.getJobConfig();
    this.pipelineMetricSummary = new PipelineMetricSummary(jobConfig.getJobId());
  }

  public void setPrepare() {
    if (!status.compareAndSet(PipelineStatus.CREATING, PipelineStatus.PREPARE)) {
      throw new DataWorkflowException(
          "pipeline could not be prepare concurrently and `prepare` should be called only once");
    }
    startTime = System.nanoTime();
  }

  public void setRunning() {
    if (!status.compareAndSet(PipelineStatus.PREPARE, PipelineStatus.RUNNING)) {
      throw new DataWorkflowException("pipeline could not be RUNNING concurrently");
    }
  }

  public void setFailed() {
    PipelineStatus pipelineStatus = status.get();
    if (pipelineStatus.isStarted()) {
      status.set(PipelineStatus.FAILED);
    }
    endTime = System.nanoTime();
  }

  public void setSuccess() {
    if (!status.compareAndSet(PipelineStatus.RUNNING, PipelineStatus.SUCCESS)) {
      throw new DataWorkflowException("pipeline could not be SUCCESS concurrently");
    }
    endTime = System.nanoTime();
  }

  public void setCanceled() {
    PipelineStatus pipelineStatus = status.get();
    if (pipelineStatus.isStarted()) {
      status.set(PipelineStatus.CANCELED);
    }
    endTime = System.nanoTime();
  }

  public PipelineStatus getStatus() {
    return this.status.get();
  }

  public long getStartNanoTime() {
    return startTime;
  }

  public long getEndNanoTime() {
    return endTime;
  }
}
