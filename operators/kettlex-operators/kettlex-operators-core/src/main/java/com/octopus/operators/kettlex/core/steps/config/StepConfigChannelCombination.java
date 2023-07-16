package com.octopus.operators.kettlex.core.steps.config;

import com.octopus.operators.kettlex.core.context.StepContext;
import com.octopus.operators.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.operators.kettlex.core.row.channel.Channel;
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
public class StepConfigChannelCombination<C extends StepConfig<?>> {
  private C stepConfig;
  private List<Channel> outputChannels;
  private Channel inputChannel;
  private StepContext stepContext;

  public void verify() {
    stepConfig.verify();
    if (stepConfig instanceof ReaderConfig && CollectionUtils.isEmpty(outputChannels)) {
      throw new KettleXStepConfigException("");
    }
    if (stepConfig instanceof TransformerConfig
        && (CollectionUtils.isEmpty(outputChannels) || inputChannel == null)) {
      throw new KettleXStepConfigException("");
    }
    if (stepConfig instanceof WriterConfig && inputChannel == null) {
      throw new KettleXStepConfigException("");
    }
  }
}
