package io.github.octopus.sys.salus.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.octopus.sys.salus.entity.SysUser;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SysUserDao extends BaseMapper<SysUser> {}
