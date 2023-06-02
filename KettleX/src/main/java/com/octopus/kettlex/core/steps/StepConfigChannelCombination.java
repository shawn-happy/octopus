package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.channel.Channel;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.model.RuntimeConfig;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.WriterConfig;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StepConfigChannelCombination implements Verifyable {
  private StepConfig<?> stepConfig;
  private List<Channel> outputChannels;
  private Channel inputChannel;

  @Override
  public void verify() {
    stepConfig.verify();
    if (stepConfig instanceof RuntimeConfig && CollectionUtils.isEmpty(outputChannels)) {
      throw new KettleXStepConfigException("");
    }
    if (stepConfig instanceof TransformationConfig
        && (CollectionUtils.isEmpty(outputChannels) || inputChannel == null)) {
      throw new KettleXStepConfigException("");
    }
    if (stepConfig instanceof WriterConfig && inputChannel == null) {
      throw new KettleXStepConfigException("");
    }
  }
}
