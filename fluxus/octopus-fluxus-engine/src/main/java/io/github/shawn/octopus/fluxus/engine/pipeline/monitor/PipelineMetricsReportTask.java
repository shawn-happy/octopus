package io.github.shawn.octopus.fluxus.engine.pipeline.monitor;

public class PipelineMetricsReportTask implements Runnable {

  private final PipelineMetricsReport pipelineMetricsReport;
  private final PipelineMetricSummary pipelineMetricSummary;

  public PipelineMetricsReportTask(
      PipelineMetricsReport pipelineMetricsReport, PipelineMetricSummary pipelineMetricSummary) {

    this.pipelineMetricsReport = pipelineMetricsReport;
    this.pipelineMetricSummary = pipelineMetricSummary;
  }

  @Override
  public void run() {
    pipelineMetricsReport.metricsReport(pipelineMetricSummary);
  }
}
