package io.github.octopus.datos.centro.sql.executor.mapper;

import io.github.octopus.datos.centro.sql.executor.entity.Privilege;
import io.github.octopus.datos.centro.sql.executor.entity.User;
import io.github.octopus.datos.centro.sql.executor.entity.User.PasswordPolicyExpire;
import io.github.octopus.datos.centro.sql.executor.entity.User.PasswordPolicyFailedLoginAttempts;
import io.github.octopus.datos.centro.sql.executor.entity.User.PasswordPolicyHistory;
import io.github.octopus.datos.centro.sql.executor.entity.User.PasswordPolicyLockTime;
import io.github.octopus.datos.centro.sql.model.PasswordPolicy;
import io.github.octopus.datos.centro.sql.model.PrivilegeInfo;
import io.github.octopus.datos.centro.sql.model.PrivilegeType;
import io.github.octopus.datos.centro.sql.model.UserInfo;
import io.github.octopus.datos.centro.sql.model.UserInfo.PasswordPolicyInfo;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisPasswordPolicy;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLPasswordPolicy;
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
                privilegeInfo
                    .getPrivileges()
                    .stream()
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
