package io.github.octopus.sys.salus.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.octopus.sys.salus.entity.SysDept;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface SysDeptDao extends BaseMapper<SysDept> {

  SysDept selectByCode(@Param("code") String code);

  List<SysDept> selectByPid(@Param("pid") Long pid);

  void updateDeptChildren(@Param("depts") List<SysDept> depts);
}
