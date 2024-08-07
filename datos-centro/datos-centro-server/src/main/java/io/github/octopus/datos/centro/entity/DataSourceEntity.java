package io.github.octopus.datos.centro.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("dc_datasource")
public class DataSourceEntity extends BaseEntity {

  @TableId(type = IdType.AUTO)
  private Long id;

  private String code;
  private String name;
  private String description;
  private String type;
  private String config;
}
