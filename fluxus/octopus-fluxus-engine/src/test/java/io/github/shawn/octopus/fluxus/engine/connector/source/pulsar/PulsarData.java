package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import lombok.Builder;
import lombok.Getter;

/** 模拟了数据中台putData的数据结构 */
@Getter
@Builder
public class PulsarData {
  private String sendTime;
  private String eventTime;
  private String operate;
  private Object data;
}
