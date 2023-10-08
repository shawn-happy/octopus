package com.octopus.actus.connector.jdbc.dao;

import com.octopus.actus.connector.jdbc.entity.Privilege;
import com.octopus.actus.connector.jdbc.entity.User;
import java.util.List;

public interface DCLDao {

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
