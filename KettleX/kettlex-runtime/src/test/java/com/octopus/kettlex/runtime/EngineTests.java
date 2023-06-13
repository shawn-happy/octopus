package com.octopus.kettlex.runtime;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.io.Resources;
import com.octopus.kettlex.runtime.config.JobConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class EngineTests {

  @Test
  public void testBuildTaskConfiguration() throws Exception {
    Engine engine = new Engine();
    String configBase64 =
        Base64.getEncoder()
            .encodeToString(
                IOUtils.toString(Resources.getResource("simple.yaml"), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8));
    JobConfiguration jobConfiguration = engine.buildJobConfiguration(configBase64);
    assertNotNull(jobConfiguration);
    engine.start(configBase64);
  }
}
