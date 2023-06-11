package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.row.channel.Channel;
import com.octopus.kettlex.model.StepConfig;
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
