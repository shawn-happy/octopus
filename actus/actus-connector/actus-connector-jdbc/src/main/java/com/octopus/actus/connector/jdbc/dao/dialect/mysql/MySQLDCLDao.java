package com.octopus.actus.connector.jdbc.dao.dialect.mysql;

import com.octopus.actus.connector.jdbc.dao.DCLDao;
import com.octopus.actus.connector.jdbc.entity.Privilege;
import com.octopus.actus.connector.jdbc.entity.User;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface MySQLDCLDao extends DCLDao {

  @Override
  void createRole(@Param("role") String role);

  @Override
  void createRoles(@Param("roles") List<String> roles);

  @Override
  void dropRole(String role);

  @Override
  void dropRoles(@Param("roles") List<String> roles);

  @Override
  void createUser(User user);

  @Override
  void dropUser(User user);

  @Override
  void dropUsers(@Param("users") List<User> users);

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
