package com.octopus.actus.connector.jdbc.service.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.PrivilegeInfo;
import com.octopus.actus.connector.jdbc.model.UserInfo;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLPasswordPolicy;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLPrivilegeType;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDCLService;
import com.octopus.actus.connector.jdbc.service.DataWarehouseDDLService;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseDCLServiceImpl;
import com.octopus.actus.connector.jdbc.service.impl.DataWarehouseDDLServiceImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class MySQLDCLTests extends MySQLTestsCommon {

  private DataWarehouseDCLService dataWarehouseDCLService;
  private DataWarehouseDDLService dataWarehouseDDLService;
  private static final String default_role = "role1";
  private static final List<String> default_roles = Arrays.asList("role1", "role2", "role3");

  @BeforeEach
  public void init() {
    dataWarehouseDDLService = new DataWarehouseDDLServiceImpl(properties);
    dataWarehouseDCLService = new DataWarehouseDCLServiceImpl(properties);
    DatabaseInfo databaseInfo = DatabaseInfo.builder().name(database).build();
    dataWarehouseDDLService.createDatabase(databaseInfo);
    dataWarehouseDDLService.createTable(TABLE_INFO);
  }

  @AfterEach
  public void destroy() {
    dataWarehouseDDLService.dropDatabase(database);
    dataWarehouseDCLService.dropRole(default_role);
    dataWarehouseDCLService.dropRoles(default_roles);
  }

  @Test
  public void testCreateRole() {
    dataWarehouseDCLService.createRole(default_role);
  }

  @Test
  public void testCreateRoleIfExists() {
    dataWarehouseDCLService.createRole(default_role);
    dataWarehouseDCLService.createRole(default_role);
  }

  @Test
  public void testCreateRoles() {
    dataWarehouseDCLService.createRoles(default_roles);
  }

  @Test
  public void testCreatRolesIfExists() {
    dataWarehouseDCLService.createRoles(default_roles);
    dataWarehouseDCLService.createRoles(default_roles);
  }

  @Test
  public void testDropRole() {
    dataWarehouseDCLService.createRole(default_role);
    dataWarehouseDCLService.dropRole(default_role);
  }

  @Test
  public void testDropRoleIfNotExists() {
    dataWarehouseDCLService.createRole(default_role);
    dataWarehouseDCLService.dropRole(default_role);
    dataWarehouseDCLService.dropRole(default_role);
  }

  @Test
  public void testDropRoles() {
    dataWarehouseDCLService.createRoles(default_roles);
    dataWarehouseDCLService.dropRoles(default_roles);
  }

  @Test
  public void testDropRolesIfNotExists() {
    dataWarehouseDCLService.createRoles(default_roles);
    dataWarehouseDCLService.dropRoles(default_roles);
    dataWarehouseDCLService.dropRoles(default_roles);
  }

  @Test
  public void testCreateUser() {
    // no host, no password, no role, no password policy
    UserInfo userInfo = UserInfo.builder().name("user1").build();
    dataWarehouseDCLService.createUser(userInfo);
  }

  @Test
  public void testCreateUserIfExists() {
    // no host, no password, no role, no password policy
    UserInfo userInfo = UserInfo.builder().name("user1").build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser2() {
    // no host, no role, no password policy, has password
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser3() {
    // no host, no role, has password, has password history default
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_HISTORY)
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser4() {
    // no host, no role, has password, has password history < 0
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_HISTORY)
                      .interval(-1)
                      .build()
                })
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDCLService.createUser(userInfo));
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser5() {
    // no host, no role, has password, has password history > 0
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_HISTORY)
                      .interval(1)
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser6() {
    // no host, no role, has password, has password expire default
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser7() {
    // no host, no role, has password, has password expire default
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser8() {
    // no host, no role, has password, has password expire never
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(-1)
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser9() {
    // no host, no role, has password, has password expire 1 day
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser10() {
    // no host, no role, has password, has password expire 1 day
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser11() {
    // no host, no role, has password, has password expire 1 day
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser12() {
    // no host, has role, has password, has password expire 1 day
    dataWarehouseDCLService.createRole("administrator");
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .roles(Collections.singletonList("administrator"))
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testCreateUser13() {
    // has host, has role, has password, has password expire 1 day
    dataWarehouseDCLService.createRole("administrator");
    UserInfo userInfo =
        UserInfo.builder()
            .name("user1")
            .host("localhost")
            .password("123456;a")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .roles(Collections.singletonList("administrator"))
            .build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testDropUsers() {
    List<UserInfo> userInfos =
        Arrays.asList(
            UserInfo.builder().name("user1").build(), UserInfo.builder().name("user2").build());
    userInfos.forEach(dataWarehouseDCLService::createUser);
    dataWarehouseDCLService.dropUsers(userInfos);
  }

  @Test
  public void testModifyUser() {
    // no host, no role, has password, has password policy history default
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser2() {
    // no host, no role, has password, has password policy history default
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_HISTORY)
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser3() {
    // no host, no role, has password, has password policy history < 0
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_HISTORY)
                      .interval(-1)
                      .build()
                })
            .build();
    assertThrows(Exception.class, () -> dataWarehouseDCLService.modifyPassword(newUser));
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser4() {
    // no host, no role, has password, has password policy history > 0
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_HISTORY)
                      .interval(1)
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser5() {
    // no host, no role, has password, has password expire default
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser6() {
    // no host, no role, has password, has password expire never
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(-1)
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser7() {
    // no host, no role, has password, has password expire 1 day
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser8() {
    // no host, no role, no password policy, has password
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser9() {
    // no host, no role, no password policy, has password
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser10() {
    // no host, no role, no password policy, has password
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    dataWarehouseDCLService.createRole("administrator");
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .roles(Collections.singletonList("administrator"))
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testModifyUser11() {
    // no host, no role, no password policy, has password
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createRole("administrator");
    dataWarehouseDCLService.createUser(userInfo);
    UserInfo newUser =
        UserInfo.builder()
            .name("user1")
            .host("localhost")
            .passwordPolicyInfos(
                new UserInfo.PasswordPolicyInfo[] {
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.FAILED_LOGIN_ATTEMPTS)
                      .interval(3)
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_LOCK_TIME)
                      .interval(1)
                      .unit("DAY")
                      .build(),
                  UserInfo.PasswordPolicyInfo.builder()
                      .passwordPolicy(MySQLPasswordPolicy.PASSWORD_EXPIRE)
                      .interval(1)
                      .unit("DAY")
                      .build()
                })
            .roles(Collections.singletonList("administrator"))
            .build();
    dataWarehouseDCLService.modifyPassword(newUser);
    dataWarehouseDCLService.dropUser(newUser);
  }

  @Test
  public void testGrantUserPrivilege() {
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createUser(userInfo);
    final PrivilegeInfo info =
        PrivilegeInfo.builder()
            .privileges(Collections.singletonList(MySQLPrivilegeType.SELECT))
            .database(database)
            .table("mysql_meta_test")
            .catalog("internal")
            .userInfos(Collections.singletonList(userInfo))
            .build();
    dataWarehouseDCLService.grantUserPrivilege(info);
    dataWarehouseDCLService.revokeUserPrivilege(info);
    dataWarehouseDCLService.dropUser(userInfo);
  }

  @Test
  public void testGrantRolePrivilege() {
    dataWarehouseDCLService.createRole("role1");
    dataWarehouseDCLService.grantRolePrivilege(
        PrivilegeInfo.builder()
            .privileges(Arrays.asList(MySQLPrivilegeType.SELECT, MySQLPrivilegeType.INSERT))
            .roles(Collections.singletonList("role1"))
            .build());
    dataWarehouseDCLService.revokeRolePrivilege(
        PrivilegeInfo.builder()
            .privileges(Arrays.asList(MySQLPrivilegeType.SELECT, MySQLPrivilegeType.INSERT))
            .roles(Collections.singletonList("role1"))
            .build());
    dataWarehouseDCLService.dropRole("role1");
  }

  @Disabled
  @Test
  // FIXME: 这段Test不可执行，执行会导致Doris挂了
  public void testGrantRolesToUser() {
    UserInfo userInfo = UserInfo.builder().name("user1").password("123456;a").build();
    dataWarehouseDCLService.createRoles(Arrays.asList("role1", "role2"));
    dataWarehouseDCLService.createUser(
        UserInfo.builder().name("user1").password("password").build());
    dataWarehouseDCLService.grantRolePrivilege(
        PrivilegeInfo.builder()
            .roles(Collections.singletonList("role1"))
            .privileges(Collections.singletonList(MySQLPrivilegeType.SELECT))
            .database(database)
            .build());
    dataWarehouseDCLService.grantRolePrivilege(
        PrivilegeInfo.builder()
            .roles(Collections.singletonList("role2"))
            .privileges(Collections.singletonList(MySQLPrivilegeType.INSERT))
            .database(database)
            .build());
    dataWarehouseDCLService.grantRolesToUser(
        PrivilegeInfo.builder()
            .roles(Arrays.asList("role1", "role2"))
            .userInfos(Collections.singletonList(userInfo))
            .build());

    dataWarehouseDCLService.revokeRolesFromUser(
        PrivilegeInfo.builder()
            .roles(Arrays.asList("role1", "role2"))
            .userInfos(Collections.singletonList(userInfo))
            .build());
    dataWarehouseDCLService.dropUser(UserInfo.builder().name("user1").build());
    dataWarehouseDCLService.revokeRolePrivilege(
        PrivilegeInfo.builder()
            .roles(Collections.singletonList("role1"))
            .database(database)
            .privileges(Collections.singletonList(MySQLPrivilegeType.SELECT))
            .build());
    dataWarehouseDCLService.revokeRolePrivilege(
        PrivilegeInfo.builder()
            .roles(Collections.singletonList("role2"))
            .database(database)
            .privileges(Collections.singletonList(MySQLPrivilegeType.INSERT))
            .build());
    dataWarehouseDCLService.dropRoles(Arrays.asList("role1", "role2"));
  }
}
