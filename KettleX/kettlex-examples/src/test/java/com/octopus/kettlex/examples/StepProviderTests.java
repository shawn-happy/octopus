package com.octopus.kettlex.examples;

import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.core.provider.StepProviderResolver;
import com.octopus.kettlex.steps.LogMessage;
import com.octopus.kettlex.steps.LogMessageConfig;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StepProviderTests {

  @Test
  public void testLogMessageProvider() {
    ServiceLoader<StepProviderResolver> sl = ServiceLoader.load(StepProviderResolver.class);
    StepConfigStepCombination stepConfigStepCombination =
        sl.findFirst()
            .orElseThrow(() -> new RuntimeException("step provider resolver not registry"))
            .getStepConfigStepCombination("LOG-MESSAGE");
    Assertions.assertNotNull(stepConfigStepCombination);
    String type = stepConfigStepCombination.getType();
    Assertions.assertEquals("LOG-MESSAGE", type);
    Assertions.assertEquals(LogMessage.class, stepConfigStepCombination.getStepClass());
    Assertions.assertEquals(LogMessageConfig.class, stepConfigStepCombination.getStepConfigClass());
  }
}
