package com.octopus.operators.spark.declare;

import com.octopus.operators.spark.declare.sink.SinkDeclare;
import com.octopus.operators.spark.declare.source.SourceDeclare;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CommonDeclare {

  private List<SourceDeclare<?>> sources;
  private List<SinkDeclare<?>> sinks;
  private Map<String, String> params;
  private Map<String, String> sparkConf;
}
