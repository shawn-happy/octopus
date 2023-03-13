package com.shawn.octopus.spark.operators.report.registry;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsOpType;
import com.shawn.octopus.spark.operators.common.exception.OpNotFoundException;
import com.shawn.octopus.spark.operators.common.exception.RegistryException;
import com.shawn.octopus.spark.operators.common.registry.Registry;
import com.shawn.octopus.spark.operators.report.metrics.op.Op;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.ObjectUtils;

public enum OpRegistry implements Registry<BuiltinMetricsOpType, Op<?>> {
  OP_REGISTRY {
    private final Map<BuiltinMetricsOpType, Op<?>> opMap = new ConcurrentHashMap<>(2 << 8);

    @Override
    public void register(BuiltinMetricsOpType key, Op<?> value) {
      if (ObjectUtils.isEmpty(key)) {
        throw new RegistryException("op name must not be null");
      }
      if (ObjectUtils.isEmpty(value)) {
        throw new RegistryException("op must not be null");
      }
      opMap.putIfAbsent(key, value);
    }

    @Override
    public Op<?> get(BuiltinMetricsOpType key) {
      Op<?> op = opMap.get(key);
      if (op == null) {
        throw new OpNotFoundException(key.name());
      }
      return op;
    }
  }
}
