package io.github.shawn.octopus.fluxus.executor.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.shawn.octopus.fluxus.executor.entity.StepEntity;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface StepDao extends BaseMapper<StepEntity> {
  void insertBatch(@Param("steps") List<StepEntity> steps);
}
