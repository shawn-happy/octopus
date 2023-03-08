package com.shawn.octopus.spark.operators.common.declare;

import com.shawn.octopus.spark.operators.common.declare.sink.SinkDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ETLDeclare {

  private List<SourceDeclare<?>> sources;
  private List<TransformDeclare<?>> transforms;
  private List<SinkDeclare<?>> sinks;
  private Map<String, String> params;
  private Map<String, String> sparkConf;
}
