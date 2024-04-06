package io.github.shawn.octopus.fluxus.executor.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import io.github.shawn.octopus.fluxus.executor.dao.handler.StringToListTypeHandler;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("t_step")
public class StepEntity {
  private String id;

  @TableField("job_id")
  private String jobId;

  private String type;
  private String identify;
  private String name;
  private String description;

  @TableField(typeHandler = StringToListTypeHandler.class, javaType = true)
  private List<String> input;

  private String output;

  @TableField(
      value = "step_attributes",
      typeHandler = StringToListTypeHandler.class,
      javaType = true)
  private List<String> stepAttributes;

  private long createTime;
  private long updateTime;

  @TableField(exist = false)
  private transient List<StepAttributeEntity> stepAttributeEntities;
}
