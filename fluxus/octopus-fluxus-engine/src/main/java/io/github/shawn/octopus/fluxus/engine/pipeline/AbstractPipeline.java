package io.github.shawn.octopus.fluxus.engine.pipeline;

import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.PipelineException;
import io.github.shawn.octopus.fluxus.engine.connector.SinkAdapter;
import io.github.shawn.octopus.fluxus.engine.connector.SourceAdapter;
import io.github.shawn.octopus.fluxus.engine.connector.TransformAdapter;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.PipelineStatus;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import io.github.shawn.octopus.fluxus.engine.pipeline.listener.DefaultPipelineListener;
import io.github.shawn.octopus.fluxus.engine.pipeline.listener.PipelineListener;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.MicrometerPipelineMetricsReport;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricSummary;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricsReport;
import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricsReportTask;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

/** 仅支持单个Source，多个Transform串行， 单个Sink 例如：Source -> transform 1 -> transform 2 -> sink */
@Slf4j
public abstract class AbstractPipeline implements Pipeline {
  protected final JobConfig jobConfig;
  protected final Source<?> source;
  protected final List<Transform<?>> transforms;
  protected final Sink<?> sink;
  private final JobContext jobContext;
  protected volatile boolean running = false;
  protected final PipelineMetricSummary pipelineMetricSummary;
  //  private final transient ScheduledExecutorService commitScheduler;
  //  private transient ScheduledFuture<?> commitScheduledFuture;
  private final transient ScheduledExecutorService monitorScheduler;
  private transient ScheduledFuture<?> monitorScheduledFuture;
  private final PipelineMetricsReport pipelineMetricsReport;
  protected final List<PipelineListener> pipelineListeners = new ArrayList<>();

  protected AbstractPipeline(ExecutionPlan executionPlan) {
    this.jobConfig = executionPlan.getJobConfig();
    this.source = new SourceAdapter<>(executionPlan.getSource());
    this.transforms =
        CollectionUtils.isNotEmpty(executionPlan.getTransforms())
            ? executionPlan
                .getTransforms()
                .stream()
                .map(TransformAdapter::new)
                .collect(Collectors.toList())
            : null;
    this.sink = new SinkAdapter<>(executionPlan.getSink());
    this.jobContext = new JobContext(executionPlan);
    this.pipelineMetricSummary = this.jobContext.getPipelineMetricSummary();
    //    this.commitScheduler =
    //        Executors.newScheduledThreadPool(
    //            1,
    //            runnable -> {
    //              Thread thread = new Thread(runnable);
    //              thread.setDaemon(true);
    //              thread.setName(String.format("pipeline-%s-committer", jobConfig.getJobId()));
    //              return thread;
    //            });
    monitorScheduler =
        Executors.newScheduledThreadPool(
            1,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setDaemon(true);
              thread.setName(String.format("pipeline-%s-monitor", jobConfig.getJobId()));
              return thread;
            });
    pipelineMetricsReport = new MicrometerPipelineMetricsReport(jobContext);
    pipelineListeners.add(new DefaultPipelineListener(this.jobContext));
  }

  public void addListener(PipelineListener pipelineListener) {
    pipelineListeners.add(pipelineListener);
  }

  @Override
  public void prepare() throws PipelineException {
    try {
      source.init();
      if (CollectionUtils.isNotEmpty(transforms)) {
        transforms.forEach(Transform::init);
      }
      sink.init();
      //      this.commitScheduledFuture =
      //          this.commitScheduler.scheduleWithFixedDelay(
      //              () -> {
      //                synchronized (AbstractPipeline.this) {
      //                  if (running) {
      //                    try {
      //                      log.info("pipeline {} commit beginning...", jobConfig.getJobId());
      //                      long sinkStart = System.nanoTime();
      //                      boolean commit = sink.commit();
      //                      pipelineMetricSummary.incrSinkFlushTimer(System.nanoTime() -
      // sinkStart);
      //                      if (commit) {
      //                        long sourceStart = System.nanoTime();
      //                        commit = source.commit();
      //                        pipelineMetricSummary.incrSourceFlushTimer(System.nanoTime() -
      // sourceStart);
      //                        log.info("pipeline {} commit success...", jobConfig.getJobId());
      //                      }
      //                      if (!commit) {
      //                        throw new PipelineException(
      //                            String.format("pipeline %s commit error...",
      // jobConfig.getJobId()));
      //                      }
      //                    } catch (Exception e) {
      //                      log.error("pipeline {} commit error...", jobConfig.getJobId(), e);
      //                      throw new PipelineException(e);
      //                    }
      //                  }
      //                }
      //              },
      //              jobConfig.getEnv().getFlushInterval(),
      //              jobConfig.getEnv().getFlushInterval(),
      //              TimeUnit.SECONDS);
      monitorScheduledFuture =
          monitorScheduler.scheduleAtFixedRate(
              new PipelineMetricsReportTask(pipelineMetricsReport, pipelineMetricSummary),
              jobConfig.getRuntimeConfig().getMetricsInterval(),
              jobConfig.getRuntimeConfig().getMetricsInterval(),
              TimeUnit.SECONDS);
      pipelineListeners.forEach(PipelineListener::onPrepare);
      running = true;
    } catch (Exception e) {
      jobContext.setFailed();
      this.shutdown();
      throw new PipelineException(e);
    }
  }

  @Override
  public JobConfig getJobConfig() {
    return jobConfig;
  }

  @Override
  public void cancel() throws PipelineException {
    log.info("pipeline-{} cancel now...", jobConfig.getJobId());
    running = false;
  }

  @Override
  public void shutdown() throws PipelineException {
    log.info("pipeline {} shutdown now...", jobConfig.getJobId());
    running = false;
    try {
      //      if (this.commitScheduledFuture != null) {
      //        commitScheduledFuture.cancel(false);
      //        this.commitScheduler.shutdown();
      //        log.info("pipeline {} shutdown commit scheduler success...", jobConfig.getJobId());
      //      }
      if (sink.commit() && source.commit()) {
        log.info("pipeline {} last commit success...", jobConfig.getJobId());
      }
      if (monitorScheduledFuture != null) {
        monitorScheduledFuture.cancel(false);
        this.monitorScheduler.shutdown();
        log.info("pipeline {} shutdown monitor scheduler success...", jobConfig.getJobId());
      }
      source.dispose();
      log.info("pipeline {} source close resource success...", jobConfig.getJobId());
      if (CollectionUtils.isNotEmpty(transforms)) {
        transforms.forEach(Transform::dispose);
        log.info("pipeline {} transforms close resource success...", jobConfig.getJobId());
      }
      sink.dispose();
      log.info("pipeline {} sink close resource success...", jobConfig.getJobId());
      pipelineListeners.forEach(PipelineListener::onShutdown);
    } catch (Exception e) {
      throw new PipelineException(e);
    } finally {
      log.info(
          "pipeline-{} finally status: \njob-id: {}, startTime: {}, endTime: {}, status: {}, total cost time: {}",
          jobConfig.getJobId(),
          jobConfig.getJobId(),
          Duration.ofNanos(getJobContext().getStartNanoTime()).getSeconds(),
          Duration.ofNanos(getJobContext().getEndNanoTime()).getSeconds(),
          getJobContext().getStatus().name(),
          getJobContext().getPipelineMetricSummary().getTotalTimer());
      JobContext jobContext = getJobContext();
      jobContext.setCanceled();
      JobContextManagement.clear();
    }
  }

  @Override
  public void run() {
    getJobContext();
    pipelineListeners.forEach(PipelineListener::onStart);
  }

  @Override
  public PipelineStatus getStatus() {
    return JobContextManagement.getJob().getStatus();
  }

  @Override
  public JobContext getJobContext() {
    JobContext jobContext = JobContextManagement.getJob();
    if (jobContext == null) {
      JobContextManagement.setJob(this.jobContext);
      return this.jobContext;
    }
    return jobContext;
  }
}
