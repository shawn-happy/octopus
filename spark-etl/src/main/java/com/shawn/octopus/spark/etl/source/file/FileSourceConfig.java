package com.shawn.octopus.spark.etl.source.file;

import com.shawn.octopus.spark.etl.source.SourceConfiguration;

public abstract class FileSourceConfig extends SourceConfiguration {

  private String name;
  private String[] paths;

  public FileSourceConfig(String[] paths) {
    this.paths = paths;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String[] getPaths() {
    return paths;
  }
}
