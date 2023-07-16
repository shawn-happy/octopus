package com.octopus.kettlex.examples;

import com.octopus.operators.kettlex.builtin.logmessage.LogMessage;
import com.octopus.operators.kettlex.builtin.logmessage.LogMessageConfig;
import com.octopus.operators.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.operators.kettlex.core.provider.StepProviderResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StepProviderTests {

  @Test
  public void testLogMessageProvider() {
    StepProviderResolver instance = StepProviderResolver.getInstance();
    StepConfigStepCombination stepConfigStepCombination =
        instance.getStepConfigStepCombination("log-message");
    Assertions.assertNotNull(stepConfigStepCombination);
    String type = stepConfigStepCombination.getType();
    Assertions.assertEquals("log-message", type);
    Assertions.assertEquals(LogMessage.class, stepConfigStepCombination.getStepClass());
    Assertions.assertEquals(LogMessageConfig.class, stepConfigStepCombination.getStepConfigClass());
  }
}
