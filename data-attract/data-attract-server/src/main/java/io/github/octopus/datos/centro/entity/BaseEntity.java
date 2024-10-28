package io.github.octopus.datos.centro.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.TableField;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BaseEntity {

  @TableField(fill = FieldFill.INSERT)
  private String creator;

  @TableField(fill = FieldFill.INSERT_UPDATE)
  private String updater;

  @TableField(fill = FieldFill.INSERT, value = "create_time")
  private Long createTime;

  @TableField(fill = FieldFill.INSERT_UPDATE, value = "update_time")
  private Long updateTime;
}
