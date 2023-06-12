package com.octopus.kettlex.core.steps.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.octopus.kettlex.core.exception.KettleXException;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public final class Version implements Comparable<Version> {

  private static final Version UNKNOWN_VERSION = new Version(0, 0, 0);

  private int majorVersion;

  private int minorVersion;

  private int patchLevel;

  public Version(int majorVersion, int minorVersion, int patchLevel) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.patchLevel = patchLevel;
  }

  @JsonCreator
  public static Version of(String version) {
    if (StringUtils.isBlank(version)) {
      return UNKNOWN_VERSION;
    }
    String[] split = version.split("\\.");
    if (ArrayUtils.isEmpty(split)) {
      return UNKNOWN_VERSION;
    }
    try {
      int majorVersion = Integer.parseInt(split[0]);
      int minorVersion = Integer.parseInt(split[1]);
      int patchLevel = Integer.parseInt(split[2]);
      return new Version(majorVersion, minorVersion, patchLevel);
    } catch (Exception e) {
      throw new KettleXException(String.format("%s is not version format", version));
    }
  }

  @Override
  @JsonValue
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(majorVersion).append('.');
    sb.append(majorVersion).append('.');
    sb.append(patchLevel);
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Version version = (Version) o;
    return majorVersion == version.majorVersion
        && minorVersion == version.minorVersion
        && patchLevel == version.patchLevel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(majorVersion, minorVersion, patchLevel);
  }

  @Override
  public int compareTo(Version other) {
    if (other == this) {
      return 0;
    }

    int diff = majorVersion - other.majorVersion;
    if (diff == 0) {
      diff = minorVersion - other.minorVersion;
      if (diff == 0) {
        diff = patchLevel - other.patchLevel;
      }
    }

    return diff;
  }
}
