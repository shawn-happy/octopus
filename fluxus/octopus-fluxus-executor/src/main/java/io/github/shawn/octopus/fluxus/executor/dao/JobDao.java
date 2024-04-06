package io.github.shawn.octopus.fluxus.executor.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.shawn.octopus.fluxus.executor.entity.JobEntity;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface JobDao extends BaseMapper<JobEntity> {
  JobEntity selectOneByNameAndVersion(String name, String version);

  List<JobEntity> selectByName(String name);
}
