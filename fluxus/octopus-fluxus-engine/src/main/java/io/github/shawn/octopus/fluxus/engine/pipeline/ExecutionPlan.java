package io.github.shawn.octopus.fluxus.engine.pipeline;

import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ExecutionPlan {
  private final JobConfig jobConfig;
  private final Source<?> source;
  private final List<Transform<?>> transforms;
  private final Sink<?> sink;
}
