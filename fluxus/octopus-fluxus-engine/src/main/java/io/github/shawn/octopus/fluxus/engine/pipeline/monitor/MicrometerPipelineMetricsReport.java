package io.github.shawn.octopus.fluxus.engine.pipeline.monitor;

import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

@Slf4j
public class MicrometerPipelineMetricsReport extends AbstractPipelineMetricsReport {

  private final Iterable<Tag> sourceTags;
  private final Map<String, Iterable<Tag>> transformTagsMap;
  private final Iterable<Tag> sinkTags;

  private static final String JOB_ID = "job_id";
  private static final String JOB_NAME = "job_name";

  private static final String SOURCE_COUNTER_NAME = "job.source.read.counter";
  private static final String SOURCE_TIMER_NAME = "job.source.read.timer";
  private static final String SOURCE_FLUSH_NAME = "job.source.flush.timer";
  private static final String SOURCE_ID = "source_id";
  private static final String SOURCE_NAME = "source_name";
  private static final String SOURCE_TYPE = "source_type";

  private static final String SINK_COUNTER_NAME = "job.sink.write.counter";
  private static final String SINK_TIMER_NAME = "job.sink.write.timer";
  private static final String SINK_FLUSH_NAME = "job.sink.flush.timer";
  private static final String SINK_ID = "sink_id";
  private static final String SINK_NAME = "sink_name";
  private static final String SINK_TYPE = "sink_type";

  private static final String TRANSFORM_COUNTER_NAME = "job.transform.counter";
  private static final String TRANSFORM_TIMER_NAME = "job.transform.timer";
  private static final String TRANSFORM_ID = "transform_id";
  private static final String TRANSFORM_NAME = "transform_name";
  private static final String TRANSFORM_TYPE = "transform_type";

  private static final String TOTAL_TIMER_NAME = "job.total.timer";

  private final Timer sourceTimer;
  private final Timer sourceFlushTimer;
  private final Timer sinkTimer;
  private final Timer sinkFlushTimer;
  private final Timer totolTimer;
  private final Map<String, Timer> transformsTimerMap = new ConcurrentHashMap<>();

  public MicrometerPipelineMetricsReport(JobContext jobContext) {
    super(jobContext);
    JobConfig jobConfig = jobContext.getJobConfig();
    Iterable<Tag> totalTags =
        Arrays.asList(
            Tag.of(JOB_ID, jobConfig.getJobId()), Tag.of(JOB_NAME, jobConfig.getJobName()));

    SourceConfig<?> sourceConfig = jobConfig.getSources().get(0);
    sourceTags =
        Arrays.asList(
            Tag.of(JOB_ID, jobConfig.getJobId()),
            Tag.of(JOB_NAME, jobConfig.getJobName()),
            Tag.of(SOURCE_ID, sourceConfig.getId()),
            Tag.of(SOURCE_NAME, sourceConfig.getName()),
            Tag.of(SOURCE_TYPE, sourceConfig.getIdentifier()));

    sourceTimer =
        Timer.builder(SOURCE_TIMER_NAME)
            .tags(sourceTags)
            .description("source read cost time")
            .register(Metrics.globalRegistry);

    sourceFlushTimer =
        Timer.builder(SOURCE_FLUSH_NAME)
            .tags(sourceTags)
            .description("source flush cost time")
            .register(Metrics.globalRegistry);

    List<TransformConfig<?>> transforms = jobConfig.getTransforms();
    transformTagsMap = new ConcurrentHashMap<>();
    if (CollectionUtils.isNotEmpty(transforms)) {
      for (TransformConfig<?> transformConfig : transforms) {
        Iterable<Tag> transformTags =
            Arrays.asList(
                Tag.of(JOB_ID, jobConfig.getJobId()),
                Tag.of(JOB_NAME, jobConfig.getJobName()),
                Tag.of(TRANSFORM_ID, transformConfig.getId()),
                Tag.of(TRANSFORM_NAME, transformConfig.getName()),
                Tag.of(TRANSFORM_TYPE, transformConfig.getIdentifier()));
        transformTagsMap.put(transformConfig.getId(), transformTags);

        transformsTimerMap.put(
            transformConfig.getId(),
            Timer.builder(TRANSFORM_TIMER_NAME)
                .tags(transformTags)
                .register(Metrics.globalRegistry));
      }
    }
    SinkConfig<?> sinkConfig = jobConfig.getSink();
    sinkTags =
        Arrays.asList(
            Tag.of(JOB_ID, jobConfig.getJobId()),
            Tag.of(JOB_NAME, jobConfig.getJobName()),
            Tag.of(SINK_ID, sinkConfig.getId()),
            Tag.of(SINK_NAME, sinkConfig.getName()),
            Tag.of(SINK_TYPE, sinkConfig.getIdentifier()));

    sinkTimer =
        Timer.builder(SINK_TIMER_NAME)
            .tags(sinkTags)
            .description("sink write cost time")
            .register(Metrics.globalRegistry);
    sinkFlushTimer =
        Timer.builder(SINK_FLUSH_NAME)
            .tags(sinkTags)
            .description("sink flush cost time")
            .register(Metrics.globalRegistry);

    totolTimer =
        Timer.builder(TOTAL_TIMER_NAME)
            .tags(totalTags)
            .description("total cost time")
            .register(Metrics.globalRegistry);
  }

  @Override
  public void metricsReport(PipelineMetricSummary summary) {
    super.metricsReport(summary);
    log.info("upload report to prometheus....");
    Gauge.builder(SOURCE_COUNTER_NAME, summary::getSourceCount)
        .tags(sourceTags)
        .description("source read count")
        .register(Metrics.globalRegistry);
    sourceTimer.record(Duration.ofNanos(summary.getSourceTimer()));
    sourceFlushTimer.record(Duration.ofNanos(summary.getSourceFlushTimer()));

    Gauge.builder(SINK_COUNTER_NAME, summary::getSinkCount)
        .tags(sinkTags)
        .description("sink write count")
        .register(Metrics.globalRegistry);
    sinkTimer.record(Duration.ofNanos(summary.getSinkTimer()));
    sinkFlushTimer.record(Duration.ofNanos(summary.getSinkFlushTimer()));

    if (MapUtils.isNotEmpty(transformTagsMap)) {
      Set<String> transformIds = transformTagsMap.keySet();
      for (String transformId : transformIds) {
        Timer timer = transformsTimerMap.get(transformId);
        Gauge.builder(TRANSFORM_COUNTER_NAME, () -> summary.getTransformCount(transformId))
            .tags(transformTagsMap.get(transformId))
            .description("transform count")
            .register(Metrics.globalRegistry);
        timer.record(Duration.ofNanos(summary.getTransformTimer(transformId)));
      }
    }
    totolTimer.record(Duration.ofNanos(summary.getTotalTimer()));
  }
}
