package io.github.shawn.octopus.fluxus.engine.pipeline.monitor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import lombok.Getter;
import org.apache.commons.collections.MapUtils;

@Getter
public class PipelineMetricSummary {
  private static final String LINE = System.lineSeparator();
  private final String jobId;
  private final DoubleAdder sourceCount = new DoubleAdder();
  private final Map<String, DoubleAdder> transformsCount = new ConcurrentHashMap<>();
  private final DoubleAdder sinkCount = new DoubleAdder();
  private final DoubleAdder sourceTimer = new DoubleAdder();
  private final Map<String, DoubleAdder> transformsTimer = new ConcurrentHashMap<>();
  private final DoubleAdder sinkTimer = new DoubleAdder();
  private final DoubleAdder totalTimer = new DoubleAdder();
  private final DoubleAdder sourceFlushTimer = new DoubleAdder();
  private final DoubleAdder sinkFlushTimer = new DoubleAdder();

  public PipelineMetricSummary(String jobId) {
    this.jobId = jobId;
  }

  public void incrSourceCount(long count) {
    sourceCount.add(count);
  }

  public double getSourceCount() {
    return sourceCount.doubleValue();
  }

  public void incrTransformCount(String transformId, long count) {
    DoubleAdder doubleAdder =
        transformsCount.computeIfAbsent(transformId, key -> new DoubleAdder());
    doubleAdder.add(count);
  }

  public double getTransformCount(String transformId) {
    DoubleAdder doubleAdder = transformsCount.get(transformId);
    if (doubleAdder == null) {
      return 0.0;
    }
    return doubleAdder.doubleValue();
  }

  public void incrSinkCount(long count) {
    sinkCount.add(count);
  }

  public double getSinkCount() {
    return sinkCount.doubleValue();
  }

  public void incrSourceTimer(double timer) {
    sourceTimer.add(timer);
  }

  public long getSourceTimer() {
    return sourceTimer.longValue();
  }

  public void incrTransformTimer(String transformId, double timer) {
    DoubleAdder doubleAdder =
        transformsTimer.computeIfAbsent(transformId, key -> new DoubleAdder());
    doubleAdder.add(timer);
  }

  public long getTransformTimer(String transformId) {
    DoubleAdder doubleAdder = transformsTimer.get(transformId);
    if (doubleAdder == null) {
      return 0L;
    }
    return doubleAdder.longValue();
  }

  public void incrSinkTimer(double timer) {
    sinkTimer.add(timer);
  }

  public long getSinkTimer() {
    return sinkTimer.longValue();
  }

  public void incrTotalTimer(double timer) {
    totalTimer.add(timer);
  }

  public long getTotalTimer() {
    return totalTimer.longValue();
  }

  public void incrSourceFlushTimer(double timer) {
    sourceFlushTimer.add(timer);
  }

  public long getSourceFlushTimer() {
    return sourceFlushTimer.longValue();
  }

  public void incrSinkFlushTimer(double timer) {
    sinkFlushTimer.add(timer);
  }

  public long getSinkFlushTimer() {
    return sinkFlushTimer.longValue();
  }

  @Override
  public String toString() {
    StringBuilder builder =
        new StringBuilder()
            .append("Pipeline Progress Information")
            .append(LINE)
            .append("job_id: ")
            .append(jobId)
            .append(LINE)
            .append("Read Count: ")
            .append(getSourceCount())
            .append(LINE)
            .append("Read Cost: ")
            .append(getSourceTimer())
            .append("s")
            .append(LINE)
            .append("Read Flush Cost: ")
            .append(getSourceFlushTimer())
            .append("s")
            .append(LINE);
    if (MapUtils.isNotEmpty(transformsCount)) {
      Set<String> transformIds = transformsCount.keySet();
      for (String transformId : transformIds) {
        double transformCount = getTransformCount(transformId);
        double transformTimer = getTransformTimer(transformId);
        builder
            .append("Transform ")
            .append(transformId)
            .append(" Count: ")
            .append(transformCount)
            .append(" Cost: ")
            .append(transformTimer)
            .append("s")
            .append(LINE);
      }
    }

    builder
        .append("Write Count: ")
        .append(getSinkCount())
        .append(LINE)
        .append("Write Cost: ")
        .append(getSinkTimer())
        .append("s")
        .append(LINE)
        .append("Write Flush Cost: ")
        .append(getSinkFlushTimer())
        .append("s")
        .append(LINE);

    builder.append("Total Cost: ").append(getTotalTimer()).append("s").append(LINE);

    return builder.toString();
  }
}
