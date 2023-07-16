package com.octopus.operators.kettlex.core.context;

import com.octopus.operators.kettlex.core.management.Communication;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DefaultStepContext implements StepContext {

  private final String stepName;
  private final Communication communication;

  @Override
  public void updateCommunication() {}

  @Override
  public void reportCommunication() {}
}
