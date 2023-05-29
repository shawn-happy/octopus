package com.octopus.kettlex.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RuntimeConfig {

  private Integer channelCapcacity;
  private Map<String, String> params;
}
