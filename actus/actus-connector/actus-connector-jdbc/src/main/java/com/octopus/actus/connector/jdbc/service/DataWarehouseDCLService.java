package com.octopus.actus.connector.jdbc.service;

import com.octopus.actus.connector.jdbc.model.PrivilegeInfo;
import com.octopus.actus.connector.jdbc.model.UserInfo;
import java.util.List;

public interface DataWarehouseDCLService {
  void createRole(String role);

  void createRoles(List<String> roles);

  void dropRole(String role);

  void dropRoles(List<String> roles);

  void createUser(UserInfo userInfo);

  void dropUser(UserInfo userInfo);

  void dropUsers(List<UserInfo> userInfos);

  void modifyPassword(UserInfo userInfo);

  void grantUserPrivilege(PrivilegeInfo privilegeInfo);

  void grantRolePrivilege(PrivilegeInfo privilegeInfo);

  void grantRolesToUser(PrivilegeInfo privilegeInfo);

  void revokeUserPrivilege(PrivilegeInfo privilegeInfo);

  void revokeRolePrivilege(PrivilegeInfo privilegeInfo);

  void revokeRolesFromUser(PrivilegeInfo privilegeInfo);
}
