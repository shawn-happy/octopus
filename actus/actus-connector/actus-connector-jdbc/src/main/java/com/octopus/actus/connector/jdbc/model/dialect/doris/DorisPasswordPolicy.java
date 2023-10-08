package com.octopus.actus.connector.jdbc.model.dialect.doris;

import com.octopus.actus.connector.jdbc.model.PasswordPolicy;
import java.util.Arrays;
import lombok.Getter;

public enum DorisPasswordPolicy implements PasswordPolicy {
  PASSWORD_HISTORY("PASSWORD_HISTORY"),
  PASSWORD_EXPIRE("PASSWORD_EXPIRE"),
  FAILED_LOGIN_ATTEMPTS("FAILED_LOGIN_ATTEMPTS"),
  PASSWORD_LOCK_TIME("PASSWORD_LOCK_TIME"),
  ;
  @Getter private final String policy;

  DorisPasswordPolicy(String policy) {
    this.policy = policy;
  }

  public static DorisPasswordPolicy of(String passwordPolicy) {
    return Arrays.stream(values())
        .filter(policy -> policy.getPolicy().equalsIgnoreCase(passwordPolicy))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "the password policy [%s] is not supported with doris", passwordPolicy)));
  }
}
