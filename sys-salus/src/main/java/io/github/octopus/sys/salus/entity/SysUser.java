package io.github.octopus.sys.salus.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("sys_user")
public class SysUser extends BaseEntity {

  @TableId(type = IdType.AUTO)
  private Long id;

  private String username;
  private String password;
  private String nickName;
  private String phone;
  private String email;
  private String userType;
  private Integer gender;
  // 头像地址
  private String avatar;
  private boolean enabled;

  private Long deptId;
}
