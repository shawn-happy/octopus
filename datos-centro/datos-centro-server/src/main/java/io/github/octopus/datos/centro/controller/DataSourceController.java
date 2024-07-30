package io.github.octopus.datos.centro.controller;

import io.github.octopus.datos.centro.model.request.datasource.CreateDataSourceRequest;
import io.github.octopus.datos.centro.model.response.ApiResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/datasource")
public class DataSourceController {

  @PostMapping("/create")
  public ApiResponse<Long> createDataSource(@RequestBody CreateDataSourceRequest<?> request){
    return null;
  }

}
