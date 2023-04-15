package com.octopus.spark.operators.declare;

import com.octopus.spark.operators.declare.check.CheckDeclare;
import com.octopus.spark.operators.declare.postprocess.PostProcessDeclare;
import com.octopus.spark.operators.declare.transform.MetricsDeclare;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class DataQualityDeclare extends CommonDeclare {

  private List<MetricsDeclare<?>> metrics;
  private List<CheckDeclare<?>> checks;
  private List<PostProcessDeclare<?>> postProcesses;
  private String filePath;
}
