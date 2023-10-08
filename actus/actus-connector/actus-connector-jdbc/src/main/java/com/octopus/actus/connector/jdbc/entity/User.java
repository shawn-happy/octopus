package com.octopus.actus.connector.jdbc.entity;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {
  private String name;
  private String host;
  private String password;
  private PasswordPolicyHistory historyPolicy;
  private PasswordPolicyExpire expirePolicy;
  private PasswordPolicyFailedLoginAttempts failedLoginPolicy;
  private PasswordPolicyLockTime lockTimePolicy;
  // for mysql
  private List<String> roles;

  // for doris
  private String role;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PasswordPolicyHistory {
    private final String policy = "PASSWORD_HISTORY";
    private Integer interval;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PasswordPolicyExpire {
    private final String policy = "PASSWORD_EXPIRE";
    private Integer interval;
    private String unit;
    private boolean never;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PasswordPolicyFailedLoginAttempts {
    private final String policy = "FAILED_LOGIN_ATTEMPTS ";
    private Integer interval;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PasswordPolicyLockTime {
    private final String policy = "PASSWORD_LOCK_TIME";
    private Integer interval;
    private String unit;
  }
}
