package com.octopus.kettlex.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.io.Resources;
import com.octopus.kettlex.core.steps.config.StepConfigChannelCombination;
import com.octopus.kettlex.runtime.config.JobConfiguration;
import com.octopus.kettlex.runtime.config.TaskGroup;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TaskGroupTests {

  private static JobConfiguration jobConfiguration;
  private static TaskGroup taskGroup;

  @BeforeAll
  public static void init() throws Exception {
    Engine engine = new Engine();
    String configBase64 =
        Base64.getEncoder()
            .encodeToString(
                IOUtils.toString(Resources.getResource("simple.yaml"), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8));
    jobConfiguration = engine.buildJobConfiguration(configBase64);
    assertNotNull(jobConfiguration);
    taskGroup = new TaskGroup(jobConfiguration);
  }

  @Test
  public void testSteps() {
    List<StepConfigChannelCombination<?>> steps = taskGroup.getSteps();
    assertNotNull(steps);
    assertEquals(3, steps.size());
    for (StepConfigChannelCombination<?> step : steps) {
      String type = step.getStepConfig().getType();
      if ("log-message".equals(type)) {
        assertNotNull(step.getInputChannel());
        assertNull(step.getOutputChannels());
      }

      if ("row-generator".equals(type)) {
        assertNull(step.getInputChannel());
        assertNotNull(step.getOutputChannels());
        assertEquals(1, step.getOutputChannels().size());
      }

      if ("value-mapper".equals(type)) {
        assertNotNull(step.getInputChannel());
        assertNotNull(step.getOutputChannels());
        assertEquals(1, step.getOutputChannels().size());
      }
    }
  }
}
