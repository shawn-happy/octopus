package com.octopus.operators.spark.registry;

import com.octopus.operators.spark.runtime.step.transform.metrics.op.ListObjectOp;
import com.octopus.operators.spark.runtime.step.transform.metrics.op.MapObjectOp;
import com.octopus.operators.spark.runtime.step.transform.metrics.op.SingleObjectOp;

public class DefaultLoader implements Loader {

  private final OpRegistry opRegistry;

  public DefaultLoader(OpRegistry opRegistry) {
    this.opRegistry = opRegistry;
  }

  @Override
  public void init() {
    initOp();
  }

  private void initOp() {
    for (SingleObjectOp op : SingleObjectOp.values()) {
      opRegistry.register(op.getOpType(), op);
    }

    for (ListObjectOp op : ListObjectOp.values()) {
      opRegistry.register(op.getOpType(), op);
    }

    for (MapObjectOp op : MapObjectOp.values()) {
      opRegistry.register(op.getOpType(), op);
    }
  }
}
