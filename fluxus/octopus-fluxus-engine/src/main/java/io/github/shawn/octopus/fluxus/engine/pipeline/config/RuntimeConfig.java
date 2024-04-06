package io.github.shawn.octopus.fluxus.engine.pipeline.config;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RuntimeConfig {
  private Long batchSize = 1000L;
  // 单位毫秒
  private Long flushInterval = 5000L;
  // 单位秒
  private Long metricsInterval = 10L;
  private Map<String, Object> extra;
}
