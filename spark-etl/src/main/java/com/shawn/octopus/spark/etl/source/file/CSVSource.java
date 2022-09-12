package com.shawn.octopus.spark.etl.source.file;

import com.shawn.octopus.spark.etl.core.StepContext;

public class CSVSource extends FileSource {

  public CSVSource(StepContext context, FileSourceConfig config) {
    super(context, config);
  }
}
