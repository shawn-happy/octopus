package io.github.shawn.octopus.fluxus.executor.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("t_step_attribute")
public class StepAttributeEntity {
  @TableId(type = IdType.INPUT)
  private String id;

  @TableField("job_id")
  private String jobId;

  @TableField("step_id")
  private String stepId;

  private String code;
  private String value;
  private long createTime;
  private long updateTime;
}
