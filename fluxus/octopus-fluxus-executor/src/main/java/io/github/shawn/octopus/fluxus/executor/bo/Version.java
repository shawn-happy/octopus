package io.github.shawn.octopus.fluxus.executor.bo;

import java.util.concurrent.atomic.AtomicInteger;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;

public class Version implements Comparable<Version> {

  private static final int INITIAL_VERSION = 0;

  private int majorVersion;
  private final AtomicInteger versionIncr;

  private Version(int majorVersion) {
    this.majorVersion = majorVersion;
    this.versionIncr = new AtomicInteger(majorVersion);
  }

  public int getCurrentVersion() {
    return majorVersion;
  }

  public void next() {
    this.majorVersion = versionIncr.incrementAndGet();
  }

  @Override
  public String toString() {
    return "V" + majorVersion;
  }

  /**
   * 获取初始版本
   *
   * @return 初始版本
   */
  public static Version getInitialVersion() {
    return new Version(INITIAL_VERSION);
  }

  public static Version of(String version) {
    if (StringUtils.isNotBlank(version) && version.startsWith("V")) {
      String versionNum = version.substring(1);
      if (!StringUtils.isNumeric(versionNum)) {
        throw new RuntimeException(String.format("version [%s] format error", version));
      }
      return new Version(Integer.parseInt(versionNum));
    }
    throw new RuntimeException(String.format("version [%s] format error", version));
  }

  public static Version of(int version) {
    if (version <= 0) {
      throw new RuntimeException(String.format("version [%d] must greater than zero", version));
    }
    return new Version(version);
  }

  @Override
  public int compareTo(@NotNull Version o) {
    if (o == this) {
      return 0;
    } else {
      return this.majorVersion - o.majorVersion;
    }
  }
}
