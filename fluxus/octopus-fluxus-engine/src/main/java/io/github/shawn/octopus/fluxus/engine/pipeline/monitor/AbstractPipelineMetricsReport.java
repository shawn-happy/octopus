package io.github.shawn.octopus.fluxus.engine.pipeline.monitor;

import io.github.shawn.octopus.fluxus.engine.common.utils.DateTimeUtils;
import io.github.shawn.octopus.fluxus.engine.common.utils.StringFormatUtils;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import java.time.Duration;
import java.time.LocalDateTime;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractPipelineMetricsReport implements PipelineMetricsReport {

  private LocalDateTime lastRunTime = LocalDateTime.now();

  protected final JobContext jobContext;

  protected AbstractPipelineMetricsReport(JobContext jobContext) {
    this.jobContext = jobContext;
  }

  @Override
  public void metricsReport(PipelineMetricSummary summary) {
    LocalDateTime now = LocalDateTime.now();
    long readTimer =
        Duration.ofNanos(summary.getSourceTimer() + summary.getSourceFlushTimer()).getSeconds();
    if (readTimer == 0L) {
      readTimer = 1L;
    }
    long writeTimer =
        Duration.ofNanos(summary.getSinkTimer() + summary.getSinkFlushTimer()).getSeconds();
    if (writeTimer == 0L) {
      writeTimer = 1L;
    }
    log.info(
        StringFormatUtils.formatTable(
            "Job Progress Information",
            "Job Id",
            jobContext.getJobConfig().getJobId(),
            "Read Count So Far",
            summary.getSourceCount(),
            "Write Count So Far",
            summary.getSinkCount(),
            "Average Read Count",
            summary.getSourceCount() / readTimer + "/s",
            "Average Write Count",
            summary.getSinkCount() / writeTimer + "/s",
            "Last Statistic Time",
            DateTimeUtils.toString(lastRunTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
            "Current Statistic Time",
            DateTimeUtils.toString(now, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)));
    lastRunTime = now;
  }
}
