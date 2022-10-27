package com.shawn.octopus.spark.etl.core.factory;

import com.shawn.octopus.spark.etl.core.enums.Format;
import com.shawn.octopus.spark.etl.source.Source;
import com.shawn.octopus.spark.etl.source.SourceOptions;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSource;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSourceOptions;
import com.shawn.octopus.spark.etl.source.file.parquet.ParquetSource;
import com.shawn.octopus.spark.etl.source.file.parquet.ParquetSourceOptions;

public final class DefaultStepFactory implements StepFactory {

  private DefaultStepFactory() {}

  public static StepFactory getStepFactory() {
    return DefaultStepFactoryHolder.STEP_FACTORY;
  }

  @Override
  public Source createSource(String name, Format format, SourceOptions config) {
    Source source = null;
    switch (format) {
      case csv:
        source = new CSVSource(name, (CSVSourceOptions) config);
        break;
      case parquet:
        source = new ParquetSource(name, (ParquetSourceOptions) config);
        break;
    }
    return source;
  }

  private static class DefaultStepFactoryHolder {
    private static final StepFactory STEP_FACTORY = new DefaultStepFactory();
  }
}
