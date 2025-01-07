package io.github.octopus.sys.salus.controller;

import io.github.octopus.sys.salus.model.reponse.ApiResult;
import io.github.octopus.sys.salus.model.request.DeptForm;
import io.github.octopus.sys.salus.service.SysDeptService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/sys/dept")
public class SysDeptController {

  private final SysDeptService sysDeptService;

  @PostMapping
  public ApiResult<Long> createDept(@RequestBody DeptForm deptForm) {
    Long dept = sysDeptService.createDept(deptForm);
    return ApiResult.success(dept);
  }
}
