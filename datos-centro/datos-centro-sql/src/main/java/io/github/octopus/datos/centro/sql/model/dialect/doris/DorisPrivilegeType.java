package io.github.octopus.datos.centro.sql.model.dialect.doris;

import com.google.common.collect.ImmutableList;
import io.github.octopus.datos.centro.sql.model.PrivilegeType;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

public enum DorisPrivilegeType implements PrivilegeType {
  /** 集群节点操作权限，包括节点上下线等操作。同时拥有 Grant_priv 和 Node_priv 的用户，可以将该权限赋予其他用户。 */
  NODE_PRIV("NODE_PRIV"),
  /** 除 NODE_PRIV 以外的所有权限。 */
  ADMIN_PRIV("ADMIN_PRIV"),
  /** 操作权限的权限。包括创建删除用户、角色，授权和撤权，设置密码等。 */
  GRANT_PRIV("GRANT_PRIV"),
  /** 对指定的库或表的读取权限 */
  SELECT_PRIV("SELECT_PRIV"),
  /** 对指定的库或表的导入权限 */
  LOAD_PRIV("LOAD_PRIV"),
  /** 对指定的库或表的schema变更权限 */
  ALTER_PRIV("ALTER_PRIV"),
  /** 对指定的库或表的创建权限 */
  CREATE_PRIV("CREATE_PRIV"),
  /** 对指定的库或表的删除权限 */
  DROP_PRIV("DROP_PRIV"),
  /** 对指定资源的使用权限<version since="dev" type="inline" >和workload group权限</version> */
  USAGE_PRIV("USAGE_PRIV"),

  // 向下兼容旧版本，实际2.0版本已经不用了
  @Deprecated
  ALL("ALL"),
  @Deprecated
  READ_WRITE("READ_WRITE"),
  @Deprecated
  READ_ONLY("READ_ONLY");

  @Getter private final String privilege;

  DorisPrivilegeType(String privilege) {
    this.privilege = privilege;
  }

  public static DorisPrivilegeType of(@NotNull String privilege) {
    return Arrays.stream(values())
        .filter(type -> type.getPrivilege().equalsIgnoreCase(privilege))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the privilege [%s] is not supported with doris", privilege)));
  }

  private static final List<DorisPrivilegeType> ADMIN_PRIVILEGES =
      ImmutableList.of(SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV);
}
