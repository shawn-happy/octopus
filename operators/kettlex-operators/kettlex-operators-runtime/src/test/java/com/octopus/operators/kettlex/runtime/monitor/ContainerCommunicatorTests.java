package com.octopus.operators.kettlex.runtime.monitor;

import com.google.common.io.Resources;
import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.utils.YamlUtil;
import com.octopus.operators.kettlex.runtime.config.JobConfiguration;
import com.octopus.operators.kettlex.runtime.config.TaskGroup;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;

public class ContainerCommunicatorTests {

  private ContainerCommunicator containerCommunicator;

  @BeforeEach
  public void setup() throws Exception {
    String configBase64 =
        Base64.getEncoder()
            .encodeToString(
                IOUtils.toString(Resources.getResource("simple.json"), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_8));
    JobConfiguration jobConfiguration = new JobConfiguration();
    YamlUtil.toYamlNode(Base64.getDecoder().decode(configBase64.getBytes(StandardCharsets.UTF_8)))
        .ifPresent(jobConfiguration::loadYaml);
    containerCommunicator = new TaskGroupContainerCommunicator(new TaskGroup(jobConfiguration));
  }

  public void getCommunication() {

    Communication collectCommunication = containerCommunicator.getCollectCommunication();
  }
}
