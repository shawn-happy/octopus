package com.octopus.operators.engine.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageFormatUtilsTests {

  @Test
  public void testMsgFormat(){
    String msg = "job {} not found";
    String message = MessageFormatUtils.getMessage(msg, "test");
    Assertions.assertEquals("job test not found", message);
  }
}
