package com.octopus.operators.kettlex.runtime.config;

import com.octopus.operators.kettlex.core.row.channel.Channel;
import com.octopus.operators.kettlex.core.steps.config.StepConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class StepLink {

  private final String from;
  private final StepConfig<?> fromStepConfig;
  private final String to;
  private final StepConfig<?> toStepConfig;
  private final Channel channel;
}
