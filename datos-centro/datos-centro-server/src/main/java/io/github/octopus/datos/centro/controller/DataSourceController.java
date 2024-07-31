package io.github.octopus.datos.centro.controller;

import com.google.protobuf.Api;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.request.datasource.CreateDataSourceRequest;
import io.github.octopus.datos.centro.model.response.ApiResponse;
import io.github.octopus.datos.centro.service.DataSourceService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/datos/datasource")
public class DataSourceController {

  private final DataSourceService dataSourceService;

  public DataSourceController(DataSourceService dataSourceService) {
    this.dataSourceService = dataSourceService;
  }

  @PostMapping("/create")
  public ApiResponse<Long> createDataSource(@RequestBody CreateDataSourceRequest<?> request){
    DataSource dataSource = dataSourceService.crateDataSource(request.getType(), request.getName(),
        request.getDescription(), request.getConfig());
    return ApiResponse.success(dataSource.getId());
  }

}
