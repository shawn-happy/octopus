package com.octopus.kettlex.core.runtime;

import com.google.common.collect.Sets;
import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleTests {

  @Test
  public void testContainsAll() {
    HashSet<String> outputs = Sets.newHashSet("output1", "output2", "output3", "output4");
    HashSet<String> input = Sets.newHashSet("output1", "output4");
    boolean b = outputs.containsAll(input);
    Assertions.assertTrue(b);
    input = Sets.newHashSet("output1", "output4", "output5");
    Assertions.assertFalse(outputs.containsAll(input));
  }
}
