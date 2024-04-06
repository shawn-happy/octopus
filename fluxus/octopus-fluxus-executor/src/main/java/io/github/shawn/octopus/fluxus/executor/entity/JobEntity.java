package io.github.shawn.octopus.fluxus.executor.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@TableName("t_job")
public class JobEntity {
  private String id;
  private String name;
  private String description;
  private long createTime;
  private long updateTime;

  @TableField(exist = false)
  private transient List<JobVersionEntity> jobVersionEntities;
}
