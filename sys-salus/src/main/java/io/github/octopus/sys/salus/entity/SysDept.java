package io.github.octopus.sys.salus.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("sys_dept")
public class SysDept extends BaseEntity {

  @TableId(type = IdType.AUTO)
  private Long id;

  private Long pid;
  private String code;
  private String name;
  private String ancestors;
  private String leaderName;
  private String leaderPhone;
  private String leaderEmail;
  private Integer orderNum;
  private String description;
  private boolean enabled;
}
