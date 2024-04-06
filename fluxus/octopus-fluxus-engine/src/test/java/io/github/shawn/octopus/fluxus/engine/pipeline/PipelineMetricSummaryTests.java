package io.github.shawn.octopus.fluxus.engine.pipeline;

import io.github.shawn.octopus.fluxus.engine.pipeline.monitor.PipelineMetricSummary;
import org.junit.jupiter.api.Test;

public class PipelineMetricSummaryTests {
  @Test
  public void testPipelineMetricSummary() {
    PipelineMetricSummary summary = new PipelineMetricSummary("job_id");
    new Thread(
            () -> {
              summary.incrSourceCount(3);
              summary.incrSourceTimer(1.0);
              summary.incrSourceFlushTimer(2.0);
              summary.incrSinkCount(3);
              summary.incrSinkTimer(1.0);
              summary.incrSinkFlushTimer(2.0);
              summary.incrTotalTimer(4.0);
              summary.incrTransformTimer("1", 2.0);
              summary.incrTransformCount("1", 3);
            })
        .start();
    new Thread(
            () -> {
              summary.incrSourceCount(3);
              summary.incrSourceTimer(1.0);
              summary.incrSourceFlushTimer(2.0);
              summary.incrSinkCount(3);
              summary.incrSinkTimer(1.0);
              summary.incrSinkFlushTimer(2.0);
              summary.incrTotalTimer(4.0);
              summary.incrTransformTimer("1", 2.0);
              summary.incrTransformCount("1", 3);
            })
        .start();
    new Thread(
            () -> {
              summary.incrSourceCount(3);
              summary.incrSourceTimer(1.0);
              summary.incrSourceFlushTimer(2.0);
              summary.incrSinkCount(3);
              summary.incrSinkTimer(1.0);
              summary.incrSinkFlushTimer(2.0);
              summary.incrTotalTimer(4.0);
              summary.incrTransformTimer("1", 2.0);
              summary.incrTransformCount("1", 3);
            })
        .start();
    new Thread(() -> System.out.println(summary)).start();
  }
}
