package com.shawn.octopus.spark.operators.report.load;

import com.shawn.octopus.spark.operators.report.metrics.op.ListObjectOp;
import com.shawn.octopus.spark.operators.report.metrics.op.MapObjectOp;
import com.shawn.octopus.spark.operators.report.metrics.op.SingleObjectOp;
import com.shawn.octopus.spark.operators.report.registry.OpRegistry;

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
