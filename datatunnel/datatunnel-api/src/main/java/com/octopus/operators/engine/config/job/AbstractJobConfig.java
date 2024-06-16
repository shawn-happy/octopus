package com.octopus.operators.engine.config.job;

import com.octopus.operators.engine.config.step.SinkConfig;
import com.octopus.operators.engine.config.step.SourceConfig;
import com.octopus.operators.engine.config.step.TransformConfig;
import com.octopus.operators.engine.util.IdGenerator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class AbstractJobConfig implements JobConfig {

  private final String id;
  private final String name;
  private final JobEngine engine;
  private final JobMode mode;
  @Setter private List<String> jars;
  @Setter private Integer parallelism = 1;
  @Setter private List<SourceConfig<?>> sources;
  @Setter private List<TransformConfig<?>> transforms;
  @Setter private SinkConfig<?> sink;
  @Setter private Map<String, Object> extraConf;

  protected AbstractJobConfig(String name, JobEngine engine) {
    this(IdGenerator.getId(), name, engine, JobMode.BATCH);
  }

  protected AbstractJobConfig(String id, String name, JobEngine engine, JobMode mode) {
    this.id = id;
    this.name = name;
    this.engine = engine;
    this.mode = mode;
  }
}
