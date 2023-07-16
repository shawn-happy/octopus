package com.octopus.spark.operators.declare;

import com.octopus.spark.operators.declare.transform.TransformDeclare;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ETLDeclare extends CommonDeclare {

  private List<TransformDeclare<?>> transforms;
}
