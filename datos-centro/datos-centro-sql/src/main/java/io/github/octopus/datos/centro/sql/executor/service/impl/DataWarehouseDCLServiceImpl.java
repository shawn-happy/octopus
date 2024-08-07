package io.github.octopus.datos.centro.sql.executor.service.impl;

import io.github.octopus.datos.centro.sql.executor.JDBCDataSourceProperties;
import io.github.octopus.datos.centro.sql.executor.mapper.DCLMapper;
import io.github.octopus.datos.centro.sql.executor.service.DataWarehouseDCLService;
import io.github.octopus.datos.centro.sql.model.PrivilegeInfo;
import io.github.octopus.datos.centro.sql.model.UserInfo;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class DataWarehouseDCLServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseDCLService {

  public DataWarehouseDCLServiceImpl(JDBCDataSourceProperties properties) {
    super(properties);
  }

  @Override
  public void createRole(String role) {
    execute(() -> dataWarehouseDCLDao.createRole(role));
  }

  @Override
  public void createRoles(List<String> roles) {
    execute(() -> dataWarehouseDCLDao.createRoles(roles));
  }

  @Override
  public void dropRole(String role) {
    execute(() -> dataWarehouseDCLDao.dropRole(role));
  }

  @Override
  public void dropRoles(List<String> roles) {
    execute(() -> dataWarehouseDCLDao.dropRoles(roles));
  }

  @Override
  public void createUser(UserInfo userInfo) {
    execute(() -> dataWarehouseDCLDao.createUser(DCLMapper.toUser(userInfo)));
  }

  @Override
  public void dropUser(UserInfo userInfo) {
    execute(() -> dataWarehouseDCLDao.dropUser(DCLMapper.toUser(userInfo)));
  }

  @Override
  public void dropUsers(List<UserInfo> userInfos) {
    execute(() -> dataWarehouseDCLDao.dropUsers(DCLMapper.toUsers(userInfos)));
  }

  @Override
  public void modifyPassword(UserInfo userInfo) {
    execute(() -> dataWarehouseDCLDao.modifyPassword(DCLMapper.toUser(userInfo)));
  }

  @Override
  public void grantUserPrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dataWarehouseDCLDao.grantUserPrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void grantRolePrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dataWarehouseDCLDao.grantRolePrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void grantRolesToUser(PrivilegeInfo privilegeInfo) {
    final List<UserInfo> userInfos = privilegeInfo.getUserInfos();
    if (CollectionUtils.isEmpty(userInfos)) {
      throw new IllegalStateException("user info cannot be null");
    }
    execute(
        () ->
            dataWarehouseDCLDao.grantRolesToUser(
                privilegeInfo.getRoles(),
                privilegeInfo.getUserInfos().get(0).getName(),
                privilegeInfo.getUserInfos().get(0).getHost()));
  }

  @Override
  public void revokeUserPrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dataWarehouseDCLDao.revokeUserPrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void revokeRolePrivilege(PrivilegeInfo privilegeInfo) {
    execute(() -> dataWarehouseDCLDao.revokeRolePrivilege(DCLMapper.toPrivilege(privilegeInfo)));
  }

  @Override
  public void revokeRolesFromUser(PrivilegeInfo privilegeInfo) {
    final List<UserInfo> userInfos = privilegeInfo.getUserInfos();
    if (CollectionUtils.isEmpty(userInfos)) {
      throw new IllegalStateException("user info cannot be null");
    }
    execute(
        () ->
            dataWarehouseDCLDao.revokeRolesFromUser(
                privilegeInfo.getRoles(),
                privilegeInfo.getUserInfos().get(0).getName(),
                privilegeInfo.getUserInfos().get(0).getHost()));
  }
}
