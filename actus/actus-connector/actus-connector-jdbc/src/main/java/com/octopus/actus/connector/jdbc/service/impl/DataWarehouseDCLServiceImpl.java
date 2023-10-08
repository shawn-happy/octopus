package com.octopus.actus.connector.jdbc.service.impl;

import com.octopus.actus.connector.jdbc.JDBCDataSourceProperties;
import com.octopus.actus.connector.jdbc.mapper.DCLMapper;
import com.octopus.actus.connector.jdbc.model.PrivilegeInfo;
import com.octopus.actus.connector.jdbc.model.UserInfo;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDCLService;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class DataWarehouseDCLServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseDCLService {

  public DataWarehouseDCLServiceImpl(JDBCDataSourceProperties properties) {
    super(properties);
  }

  @Override
  public void createRole(String role) {
    execute(() -> dclDao.createRole(role));
  }

  @Override
  public void createRoles(List<String> roles) {
    execute(() -> dclDao.createRoles(roles));
  }

  @Override
  public void dropRole(String role) {
    execute(() -> dclDao.dropRole(role));
  }

  @Override
  public void dropRoles(List<String> roles) {
    execute(() -> dclDao.dropRoles(roles));
  }

  @Override
  public void createUser(UserInfo userInfo) {
    execute(() -> dclDao.createUser(DCLMapper.toUser(userInfo)));
  }

  @Override
  public void dropUser(UserInfo userInfo) {
    execute(() -> dclDao.dropUser(DCLMapper.toUser(userInfo)));
  }

  @Override
  public void dropUsers(List<UserInfo> userInfos) {
    execute(() -> dclDao.dropUsers(DCLMapper.toUsers(userInfos)));
  }

  @Override
  public void modifyPassword(UserInfo userInfo) {
    execute(() -> dclDao.modifyPassword(DCLMapper.toUser(userInfo)));
  }

  @Override
  public void grantUserPrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dclDao.grantUserPrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void grantRolePrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dclDao.grantRolePrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void grantRolesToUser(PrivilegeInfo privilegeInfo) {
    final List<UserInfo> userInfos = privilegeInfo.getUserInfos();
    if (CollectionUtils.isEmpty(userInfos)) {
      throw new IllegalStateException("user info cannot be null");
    }
    execute(
        () ->
            dclDao.grantRolesToUser(
                privilegeInfo.getRoles(),
                privilegeInfo.getUserInfos().get(0).getName(),
                privilegeInfo.getUserInfos().get(0).getHost()));
  }

  @Override
  public void revokeUserPrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dclDao.revokeUserPrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void revokeRolePrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dclDao.revokeRolePrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void revokeRolesFromUser(PrivilegeInfo privilegeInfo) {
    final List<UserInfo> userInfos = privilegeInfo.getUserInfos();
    if (CollectionUtils.isEmpty(userInfos)) {
      throw new IllegalStateException("user info cannot be null");
    }
    execute(
        () ->
            dclDao.revokeRolesFromUser(
                privilegeInfo.getRoles(),
                privilegeInfo.getUserInfos().get(0).getName(),
                privilegeInfo.getUserInfos().get(0).getHost()));
  }
}
