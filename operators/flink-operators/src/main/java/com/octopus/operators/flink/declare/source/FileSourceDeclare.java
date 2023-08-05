package com.octopus.operators.flink.declare.source;

import com.octopus.operators.flink.declare.common.ColumnDesc;
import com.octopus.operators.flink.declare.common.SourceType;
import com.octopus.operators.flink.declare.source.FileSourceDeclare.FileSourceOptions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public abstract class FileSourceDeclare<P extends FileSourceOptions> implements SourceDeclare<P> {

  private SourceType type;
  private P options;
  private Integer repartition;
  private String output;
  private String name;

  @SuperBuilder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public abstract static class FileSourceOptions implements SourceOptions {
    private String[] paths;
    private List<ColumnDesc> schemas;
  }
}
