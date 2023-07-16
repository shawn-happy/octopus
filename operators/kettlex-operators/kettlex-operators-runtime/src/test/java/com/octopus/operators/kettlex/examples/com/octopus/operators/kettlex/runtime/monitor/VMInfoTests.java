package com.octopus.operators.kettlex.examples.com.octopus.operators.kettlex.runtime.monitor;

import com.octopus.operators.kettlex.runtime.monitor.VMInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class VMInfoTests {

  private static VMInfo vmInfo;

  @BeforeAll
  public static void init() {
    vmInfo = VMInfo.getVmInfo();
  }

  @Test
  public void print() {
    vmInfo.getDelta(true);
  }
}
