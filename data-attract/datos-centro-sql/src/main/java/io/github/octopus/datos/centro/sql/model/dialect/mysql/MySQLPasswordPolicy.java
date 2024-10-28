package io.github.octopus.datos.centro.sql.model.dialect.mysql;

import io.github.octopus.datos.centro.sql.model.PasswordPolicy;
import java.util.Arrays;
import lombok.Getter;

public enum MySQLPasswordPolicy implements PasswordPolicy {
  PASSWORD_HISTORY("PASSWORD HISTORY"),
  PASSWORD_EXPIRE("PASSWORD EXPIRE"),
  FAILED_LOGIN_ATTEMPTS("FAILED_LOGIN_ATTEMPTS"),
  PASSWORD_LOCK_TIME("PASSWORD_LOCK_TIME"),
  ;
  @Getter private final String policy;

  MySQLPasswordPolicy(String policy) {
    this.policy = policy;
  }

  public static MySQLPasswordPolicy of(String passwordPolicy) {
    return Arrays.stream(values())
        .filter(policy -> policy.getPolicy().equalsIgnoreCase(passwordPolicy))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "the password policy [%s] is not supported with mysql", passwordPolicy)));
  }
}
