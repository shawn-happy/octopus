package io.github.shawn.octopus.fluxus.executor.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.shawn.octopus.fluxus.executor.entity.JobVersionEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface JobVersionDao extends BaseMapper<JobVersionEntity> {

  void deleteByJobDefId(@Param("jobDefId") String jobDefId);

  JobVersionEntity selectOneByNameAndVersion(
      @Param("name") String name, @Param("version") int version);
}
