package io.github.shawn.octopus.fluxus.executor.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.shawn.octopus.fluxus.executor.entity.StepAttributeEntity;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface StepAttributeDao extends BaseMapper<StepAttributeEntity> {

  void insertBatch(@Param("attributes") List<StepAttributeEntity> attributes);
}
