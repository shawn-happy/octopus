package com.octopus.operators.spark.runtime.source;

import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import com.octopus.operators.engine.table.EngineRow;
import java.util.List;

public class FakeDataGenerator {

  private final FakeSourceOptions options;

  public FakeDataGenerator(FakeSourceOptions options) {
    this.options = options;
  }

  public EngineRow random() {
    FakeSourceRow[] fields = options.getFields();
    for (FakeSourceRow field : fields) {}

    return null;
  }

  public List<Integer> fake() {
    return List.of(1, 2, 3, 4, 5);
  }
}
