package com.octopus.kettlex.runtime;

import com.octopus.kettlex.core.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class StepLink {

  private final String from;
  private final String to;
  private final Channel channel;
}
