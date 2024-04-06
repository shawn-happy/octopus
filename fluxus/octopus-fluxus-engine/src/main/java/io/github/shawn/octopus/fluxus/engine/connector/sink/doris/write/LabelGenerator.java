package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.write;

public class LabelGenerator {
  private final String labelPrefix;
  private final boolean enable2PC;

  public LabelGenerator(String labelPrefix, boolean enable2PC) {
    this.labelPrefix = labelPrefix;
    this.enable2PC = enable2PC;
  }

  public String generateLabel(long chkId) {
    return enable2PC ? labelPrefix + "_" + chkId : labelPrefix + "_" + System.currentTimeMillis();
  }
}
