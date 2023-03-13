package com.shawn.octopus.spark.operators.report.load;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsOpType;
import com.shawn.octopus.spark.operators.report.metrics.op.SingleColumnSingleResultOp;
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
    opRegistry.register(BuiltinMetricsOpType.max, SingleColumnSingleResultOp.MAX_OP);
    opRegistry.register(BuiltinMetricsOpType.min, SingleColumnSingleResultOp.MIN_OP);
  }
}
