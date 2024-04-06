package io.github.shawn.octopus.fluxus.engine.pipeline;

import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.exception.PipelineException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobMode;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Slf4j
public class SingletonSourcePipeline extends AbstractPipeline {

  private final JobMode jobMode;
  private final LongAdder longAdder = new LongAdder();

  public SingletonSourcePipeline(ExecutionPlan executionPlan) {
    super(executionPlan);
    this.jobMode = getJobConfig().getJobMode();
  }

  @Override
  public void run() {
    super.run();
    try {
      source.begin();
      sink.begin();
      while (running) {
        long startTime = System.nanoTime();
        RowRecord record = source.read();
        if (record == null) {
          if (jobMode == JobMode.BATCH) {
            running = false;
          } else {
            if (longAdder.longValue() != 0L) {
              commit();
              longAdder.reset();
            }
            TimeUnit.MILLISECONDS.sleep(jobConfig.getRuntimeConfig().getFlushInterval());
          }
          continue;
        }
        longAdder.increment();
        RowRecord newRecord = record;
        if (CollectionUtils.isNotEmpty(transforms)) {
          for (Transform<?> transform : transforms) {
            transform.begin();
            newRecord = transform.transform(newRecord);
          }
        }
        sink.write(newRecord);
        if (pipelineMetricSummary.getSourceCount() % jobConfig.getRuntimeConfig().getBatchSize()
            == 0) {
          commit();
          longAdder.reset();
        }
        pipelineMetricSummary.incrTotalTimer(System.nanoTime() - startTime);
      }
      getJobContext().setSuccess();
    } catch (Exception e) {
      getJobContext().setFailed();
      running = false;
      sink.abort();
      source.abort();
      log.error("pipeline {} error", jobConfig.getJobId(), e);
      throw new DataWorkflowException(e);
    } finally {
      this.shutdown();
    }
  }

  private void commit() {
    log.info("pipeline {} commit beginning...", jobConfig.getJobId());
    boolean commit = sink.commit();
    if (commit) {
      commit = source.commit();
      log.info("pipeline {} commit success...", jobConfig.getJobId());
    }
    if (!commit) {
      throw new PipelineException(
          String.format("pipeline %s commit error...", jobConfig.getJobId()));
    }
  }
}
