package com.octopus.operators.spark.declare;

import com.octopus.operators.spark.declare.transform.MetricsDeclare;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ReportDeclare extends CommonDeclare {
  private List<MetricsDeclare<?>> metrics;
  private String filePath;
}
