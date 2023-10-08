package com.octopus.actus.connector.jdbc.mapper;

import com.octopus.actus.connector.jdbc.entity.Privilege;
import com.octopus.actus.connector.jdbc.entity.User;
import com.octopus.actus.connector.jdbc.entity.User.PasswordPolicyExpire;
import com.octopus.actus.connector.jdbc.entity.User.PasswordPolicyFailedLoginAttempts;
import com.octopus.actus.connector.jdbc.entity.User.PasswordPolicyHistory;
import com.octopus.actus.connector.jdbc.entity.User.PasswordPolicyLockTime;
import com.octopus.actus.connector.jdbc.model.PasswordPolicy;
import com.octopus.actus.connector.jdbc.model.PrivilegeInfo;
import com.octopus.actus.connector.jdbc.model.PrivilegeType;
import com.octopus.actus.connector.jdbc.model.UserInfo;
import com.octopus.actus.connector.jdbc.model.UserInfo.PasswordPolicyInfo;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisPasswordPolicy;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLPasswordPolicy;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

public class DCLMapper {

  public static User toUser(UserInfo userInfo) {
    if (userInfo == null) {
      return null;
    }
    final PasswordPolicyInfo[] passwordPolicyInfos = userInfo.getPasswordPolicyInfos();
    PasswordPolicyHistory history = null;
    PasswordPolicyExpire expire = null;
    PasswordPolicyFailedLoginAttempts failedLoginAttempts = null;
    PasswordPolicyLockTime lockTime = null;
    if (ArrayUtils.isNotEmpty(passwordPolicyInfos)) {
      for (PasswordPolicyInfo info : passwordPolicyInfos) {
        final PasswordPolicy passwordPolicy = info.getPasswordPolicy();
        if (DorisPasswordPolicy.PASSWORD_HISTORY.equals(passwordPolicy)
            || MySQLPasswordPolicy.PASSWORD_HISTORY.equals(passwordPolicy)) {
          history = PasswordPolicyHistory.builder().interval(info.getInterval()).build();
        } else if (DorisPasswordPolicy.PASSWORD_EXPIRE.equals(passwordPolicy)
            || MySQLPasswordPolicy.PASSWORD_EXPIRE.equals(passwordPolicy)) {
          expire =
              PasswordPolicyExpire.builder()
                  .interval(info.getInterval())
                  .unit(info.getUnit())
                  .never(info.getInterval() != null && info.getInterval() < 0)
                  .build();
        } else if (DorisPasswordPolicy.FAILED_LOGIN_ATTEMPTS.equals(passwordPolicy)
            || MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS.equals(passwordPolicy)) {
          failedLoginAttempts =
              PasswordPolicyFailedLoginAttempts.builder().interval(info.getInterval()).build();
        } else if (DorisPasswordPolicy.PASSWORD_LOCK_TIME.equals(passwordPolicy)
            || MySQLPasswordPolicy.PASSWORD_LOCK_TIME.equals(passwordPolicy)) {
          lockTime =
              PasswordPolicyLockTime.builder()
                  .interval(info.getInterval())
                  .unit(info.getUnit())
                  .build();
        }
      }
    }
    return User.builder()
        .name(userInfo.getName())
        .host(userInfo.getHost())
        .password(userInfo.getPassword())
        .role(CollectionUtils.isNotEmpty(userInfo.getRoles()) ? userInfo.getRoles().get(0) : null)
        .historyPolicy(history)
        .lockTimePolicy(lockTime)
        .failedLoginPolicy(failedLoginAttempts)
        .expirePolicy(expire)
        .roles(userInfo.getRoles())
        .build();
  }

  public static List<User> toUsers(List<UserInfo> userInfos) {
    return CollectionUtils.isEmpty(userInfos)
        ? null
        : userInfos.stream().map(DCLMapper::toUser).collect(Collectors.toList());
  }

  public static Privilege toPrivilege(PrivilegeInfo privilegeInfo) {
    if (privilegeInfo == null) {
      return null;
    }
    Privilege.PrivilegeBuilder builder =
        Privilege.builder()
            .database(privilegeInfo.getDatabase())
            .table(privilegeInfo.getTable())
            .privileges(
                privilegeInfo.getPrivileges().stream()
                    .map(PrivilegeType::getPrivilege)
                    .collect(Collectors.toList()))
            .catalog(privilegeInfo.getCatalog());
    List<UserInfo> userInfos = privilegeInfo.getUserInfos();
    if (CollectionUtils.isNotEmpty(userInfos)) {
      UserInfo userInfo = userInfos.iterator().next();
      builder.user(userInfo.getName());
      builder.host(userInfo.getHost());
    }
    builder.role(
        CollectionUtils.isEmpty(privilegeInfo.getRoles())
            ? null
            : privilegeInfo.getRoles().iterator().next());
    builder.roles(privilegeInfo.getRoles());
    builder.users(toUsers(userInfos));
    return builder.build();
  }
}
