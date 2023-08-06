package com.octopus.operators.flink.runtime;

import com.octopus.operators.flink.declare.sink.SinkDeclare;
import com.octopus.operators.flink.declare.source.SourceDeclare;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlinkRuntimeConfig {
  private List<? extends SourceDeclare<?>> sources;
  private List<? extends SinkDeclare<?>> sinks;
}
