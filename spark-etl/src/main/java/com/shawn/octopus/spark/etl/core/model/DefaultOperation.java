package com.shawn.octopus.spark.etl.core.model;

import com.shawn.octopus.spark.etl.core.api.Operation;
import java.util.UUID;

public class DefaultOperation implements Operation {

  private final String id;
  private final String key;

  public DefaultOperation(String id) {
    this.id = id;
    this.key = this.id + UUID.randomUUID();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getKey() {
    return key;
  }
}
