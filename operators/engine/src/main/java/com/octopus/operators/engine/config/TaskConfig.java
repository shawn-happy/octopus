package com.octopus.operators.engine.config;

import com.octopus.operators.engine.config.sink.SinkConfig;
import com.octopus.operators.engine.config.sink.SinkOptions;
import com.octopus.operators.engine.config.source.SourceConfig;
import com.octopus.operators.engine.config.source.SourceOptions;
import com.octopus.operators.engine.config.transform.TransformConfig;
import com.octopus.operators.engine.config.transform.TransformOptions;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskConfig {

  @NotNull private String taskName;
  @Default private Integer parallelism = 1;
  @Default private TaskMode taskMode = TaskMode.BATCH;
  private long checkpointInterval;
  private String jars;
  private Map<String, Object> runtimeConfig;
  private Map<String, Object> extraParams;
  private List<SourceConfig<? extends SourceOptions>> sources;
  private List<SinkConfig<? extends SinkOptions>> sinks;
  private List<TransformConfig<? extends TransformOptions>> transforms;
}
