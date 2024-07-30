package io.github.octopus.datos.centro.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.octopus.datos.centro.entity.DataSourceEntity;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface DataSourceDao extends BaseMapper<DataSourceEntity> {

}
