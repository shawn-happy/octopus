package com.octopus.kettlex.core.provider;

import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

public class StepProviderTests {

  @Test
  public void testLogMessageProvider() {
    ServiceLoader<StepProviderResolver> sl = ServiceLoader.load(StepProviderResolver.class);
    StepConfigStepCombination stepConfigStepCombination =
        sl.findFirst()
            .orElseThrow(() -> new RuntimeException("step provider resolver not registry"))
            .getStepConfigStepCombination("LOG-MESSAGE");
  }
}
