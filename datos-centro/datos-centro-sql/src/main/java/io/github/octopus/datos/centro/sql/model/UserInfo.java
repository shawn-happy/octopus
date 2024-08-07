package io.github.octopus.datos.centro.sql.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo {
  private String name;
  private String host;
  private String password;
  private PasswordPolicyInfo[] passwordPolicyInfos;
  private List<String> roles;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PasswordPolicyInfo {
    private PasswordPolicy passwordPolicy;
    private Integer interval;
    private String unit;
  }
}
