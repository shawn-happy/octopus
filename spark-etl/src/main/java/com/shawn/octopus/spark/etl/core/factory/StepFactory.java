package com.shawn.octopus.spark.etl.core.factory;

import com.shawn.octopus.spark.etl.core.enums.Format;
import com.shawn.octopus.spark.etl.source.Source;
import com.shawn.octopus.spark.etl.source.SourceOptions;

public interface StepFactory {

  Source createSource(String name, Format format, SourceOptions config);
}
