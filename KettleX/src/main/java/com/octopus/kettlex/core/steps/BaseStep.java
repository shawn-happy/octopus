package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.channel.Channel;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class BaseStep implements Step {

  private StepMeta stepMeta;
  private StepContext stepContext;
  private Channel[] inputRowsets;
  private Channel[] outputRowsets;

  public BaseStep(StepMeta stepMeta, StepContext stepContext) {
    this.stepMeta = stepMeta;
    this.stepContext = stepContext;
  }

  @Override
  public boolean init() {
    return true;
  }
}
