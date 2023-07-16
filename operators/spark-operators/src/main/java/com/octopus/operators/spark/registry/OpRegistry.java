package com.octopus.operators.spark.registry;

import com.octopus.operators.spark.declare.transform.BuiltinMetricsOpType;
import com.octopus.operators.spark.exception.OpNotFoundException;
import com.octopus.operators.spark.exception.RegistryException;
import com.octopus.operators.spark.runtime.step.transform.metrics.op.Op;
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
