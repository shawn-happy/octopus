package io.github.octopus.sys.salus.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.octopus.sys.salus.entity.SysDept;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface SysDeptDao extends BaseMapper<SysDept> {

  SysDept selectByCode(@Param("code") String code);
}
