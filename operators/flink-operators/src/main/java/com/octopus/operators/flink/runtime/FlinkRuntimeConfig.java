package com.octopus.operators.flink.runtime;

import com.octopus.operators.flink.declare.sink.SinkDeclare;
import com.octopus.operators.flink.declare.source.SourceDeclare;
import com.octopus.operators.flink.declare.transform.TransformDeclare;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlinkRuntimeConfig {
  private String name;
  private List<? extends SourceDeclare<?>> sources;
  private List<? extends TransformDeclare<?>> transforms;
  private List<? extends SinkDeclare<?>> sinks;
  private Map<String, String> flinkConf;
}
