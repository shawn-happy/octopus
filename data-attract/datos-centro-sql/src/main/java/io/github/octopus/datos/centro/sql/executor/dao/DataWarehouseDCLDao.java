package io.github.octopus.datos.centro.sql.executor.dao;

import io.github.octopus.datos.centro.sql.executor.entity.Privilege;
import io.github.octopus.datos.centro.sql.executor.entity.User;
import java.util.List;

public interface DataWarehouseDCLDao {

  void createRole(String role);

  void createRoles(List<String> roles);

  void dropRole(String role);

  void dropRoles(List<String> roles);

  void createUser(User userInfo);

  void dropUser(User userInfo);

  void dropUsers(List<User> users);

  void modifyPassword(User user);

  void grantUserPrivilege(Privilege privilege);

  void grantRolePrivilege(Privilege privilege);

  void grantRolesToUser(List<String> roles, String user, String host);

  void revokeUserPrivilege(Privilege privilege);

  void revokeRolePrivilege(Privilege privilege);

  void revokeRolesFromUser(List<String> roles, String user, String host);
}
