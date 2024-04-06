package io.github.shawn.octopus.fluxus.engine.pipeline.dag;

import io.github.shawn.octopus.fluxus.api.config.StepConfig;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
@AllArgsConstructor
public class LogicalVertex {
  // only for sorted
  @Setter private long vertexId;
  private String name;
  private StepConfig stepConfig;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LogicalVertex that = (LogicalVertex) o;
    return Objects.equals(vertexId, that.vertexId)
        && Objects.equals(name, that.name)
        && Objects.equals(stepConfig, that.stepConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vertexId, name, stepConfig);
  }
}
