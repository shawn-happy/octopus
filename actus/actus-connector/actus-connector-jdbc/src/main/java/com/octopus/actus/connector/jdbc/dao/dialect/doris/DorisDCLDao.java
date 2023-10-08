package com.octopus.actus.connector.jdbc.dao.dialect.doris;

import com.octopus.actus.connector.jdbc.dao.DCLDao;
import com.octopus.actus.connector.jdbc.entity.Privilege;
import com.octopus.actus.connector.jdbc.entity.User;
import com.octopus.actus.connector.jdbc.exception.SqlExecutorException;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.annotations.Param;

public interface DorisDCLDao extends DCLDao {

  @Override
  void createRole(@Param("role") String role);

  @Override
  default void createRoles(List<String> roles) {
    if (CollectionUtils.isEmpty(roles)) {
      throw new SqlExecutorException("roles cannot be null");
    }
    roles.forEach(this::createRole);
  }

  @Override
  void dropRole(String role);

  @Override
  default void dropRoles(List<String> roles) {
    if (CollectionUtils.isEmpty(roles)) {
      throw new SqlExecutorException("roles cannot be null");
    }
    roles.forEach(this::dropRole);
  }

  @Override
  void createUser(User user);

  @Override
  void dropUser(User user);

  @Override
  default void dropUsers(List<User> users) {
    if (CollectionUtils.isEmpty(users)) {
      return;
    }
    users.forEach(this::dropUser);
  }

  @Override
  void modifyPassword(User user);

  @Override
  void grantUserPrivilege(Privilege privilege);

  @Override
  void grantRolePrivilege(Privilege privilege);

  @Override
  void grantRolesToUser(
      @Param("roles") List<String> roles, @Param("user") String user, @Param("host") String host);

  @Override
  void revokeUserPrivilege(Privilege privilege);

  @Override
  void revokeRolePrivilege(Privilege privilege);

  @Override
  void revokeRolesFromUser(
      @Param("roles") List<String> roles, @Param("user") String user, @Param("host") String host);
}
