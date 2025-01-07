package io.github.octopus.actus.plugin.doris.model;

import io.github.octopus.actus.core.model.schema.TabletAlgo;

public enum DorisTabletArgo implements TabletAlgo {
  Hash("HASH"),
  ;

  private final String algo;

  DorisTabletArgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }
}
