package io.github.shawn.octopus.fluxus.engine.pipeline.dag;

import lombok.Getter;

@Getter
public class LogicalEdge {
  private final String fromVertexName;
  private final String toVertexName;
  private final LogicalVertex fromVertex;
  private final LogicalVertex toVertex;

  public LogicalEdge(LogicalVertex fromVertex, LogicalVertex toVertex) {
    this.fromVertex = fromVertex;
    this.toVertex = toVertex;
    this.fromVertexName = fromVertex.getName();
    this.toVertexName = toVertex.getName();
  }
}
