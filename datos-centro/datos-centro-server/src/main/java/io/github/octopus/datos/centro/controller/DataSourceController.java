package io.github.octopus.datos.centro.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.github.octopus.datos.centro.mapper.DataSourceMapper;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.form.FormStructure;
import io.github.octopus.datos.centro.model.request.datasource.DataSourceRequest;
import io.github.octopus.datos.centro.model.request.datasource.QueryDataSourceRequest;
import io.github.octopus.datos.centro.model.response.ApiResponse;
import io.github.octopus.datos.centro.model.response.datasource.CheckResult;
import io.github.octopus.datos.centro.model.response.datasource.DataSourceDetailsVO;
import io.github.octopus.datos.centro.service.DataSourceService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/datos/datasource")
public class DataSourceController {

  private final DataSourceService dataSourceService;

  public DataSourceController(DataSourceService dataSourceService) {
    this.dataSourceService = dataSourceService;
  }

  @PostMapping("/check/connect")
  public ApiResponse<CheckResult> checkDataSourceConnect(
      @RequestBody DataSourceRequest<?> request) {
    return ApiResponse.success(
        dataSourceService.checkDataSourceConnection(request.getType(), request.getConfig()));
  }

  @PostMapping("/create")
  public ApiResponse<Long> createDataSource(@RequestBody DataSourceRequest<?> request) {
    DataSource dataSource =
        dataSourceService.crateDataSource(
            request.getType(), request.getName(), request.getDescription(), request.getConfig());
    return ApiResponse.success(dataSource.getId());
  }

  @PutMapping("/{id}")
  public ApiResponse<Boolean> editDataSource(
      @PathVariable("id") long dataSourceId,
      @RequestBody DataSourceRequest<? extends DataSourceConfig> updateDSReq) {
    return ApiResponse.success(
        dataSourceService.updateDataSource(
            dataSourceId,
            updateDSReq.getName(),
            updateDSReq.getDescription(),
            updateDSReq.getConfig()));
  }

  @DeleteMapping("/{id}")
  public ApiResponse<Integer> deleteDataSource(@PathVariable("id") long id) {
    // TODO 校验有没有使用该数据源的任务
    return ApiResponse.success(dataSourceService.deleteDataSource(id));
  }

  @GetMapping("/{id}")
  public ApiResponse<DataSourceDetailsVO> getDataSourceDetails(@PathVariable("id") long id) {
    return ApiResponse.success(
        DataSourceMapper.toDsDetailsVO(dataSourceService.queryDataSourceById(id)));
  }

  @GetMapping("/selectPage")
  public ApiResponse<IPage<DataSourceDetailsVO>> getDataSources(
      @RequestBody QueryDataSourceRequest request,
      @RequestParam("pageIndex") Long pageNo,
      @RequestParam("pageSize") Long pageSize) {
    IPage<DataSource> dataSourceIPage =
        dataSourceService.queryDataSourceList(
            request.getDsName(),
            request.getCreateBeginTime(),
            request.getCreateEndTime(),
            pageNo,
            pageSize);
    return ApiResponse.success(DataSourceMapper.dataSourceDetailsVOPageMapper(dataSourceIPage));
  }

  @GetMapping("/dynamic-form")
  public ApiResponse<FormStructure> getDataSourceForm(@RequestParam("type") String dsType) {
    return ApiResponse.success(dataSourceService.getDataSourceDynamicForm(dsType));
  }
}
