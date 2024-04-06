package io.github.shawn.octopus.fluxus.executor.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import io.github.shawn.octopus.fluxus.executor.dao.handler.StringToListTypeHandler;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@TableName("t_job_version")
public class JobVersionEntity {
  private long id;
  private String jobId;
  private String jobName;
  private int version;

  @TableField(value = "step_ids", typeHandler = StringToListTypeHandler.class, javaType = true)
  private List<String> steps;

  private long createTime;
  private long updateTime;

  @TableField(exist = false)
  private transient List<StepEntity> stepEntities;
}
