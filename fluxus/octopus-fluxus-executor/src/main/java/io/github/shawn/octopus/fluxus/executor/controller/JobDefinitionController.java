package io.github.shawn.octopus.fluxus.executor.controller;

import io.github.shawn.octopus.fluxus.executor.bo.JobDefinitionInfo;
import io.github.shawn.octopus.fluxus.executor.bo.Step;
import io.github.shawn.octopus.fluxus.executor.dto.JobConfigRequest;
import io.github.shawn.octopus.fluxus.executor.dto.JobDefinitionRequest;
import io.github.shawn.octopus.fluxus.executor.dto.UpdateJobConfigRequest;
import io.github.shawn.octopus.fluxus.executor.dto.UpdateJobDefinitionRequest;
import io.github.shawn.octopus.fluxus.executor.mapper.BoToVoMapper;
import io.github.shawn.octopus.fluxus.executor.mapper.DtoBoMapper;
import io.github.shawn.octopus.fluxus.executor.service.JobDefinitionService;
import io.github.shawn.octopus.fluxus.executor.vo.ApiResponse;
import io.github.shawn.octopus.fluxus.executor.vo.JobDefinitionVO;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/data-workflow/api/v1/job-definition")
public class JobDefinitionController {

  private final JobDefinitionService jobDefinitionService;

  public JobDefinitionController(JobDefinitionService jobDefinitionService) {
    this.jobDefinitionService = jobDefinitionService;
  }

  @PostMapping
  public ApiResponse<String> create(@RequestBody JobDefinitionRequest request) {
    JobDefinitionInfo jobDefinitionInfo = DtoBoMapper.toJobDefinition(request);
    return ApiResponse.ok(
        jobDefinitionService.createJobDefinition(
            jobDefinitionInfo.getName(), jobDefinitionInfo.getDescription()));
  }

  @GetMapping("/{jobDefId}")
  public ApiResponse<JobDefinitionVO> getById(@PathVariable("jobDefId") String jobDefId) {
    return ApiResponse.ok(BoToVoMapper.toJobDefVo(jobDefinitionService.findById(jobDefId)));
  }

  @DeleteMapping("/{jobDefId}")
  public ApiResponse<String> delete(@PathVariable("jobDefId") String jobDefId) {
    jobDefinitionService.delete(jobDefId);
    return ApiResponse.ok(jobDefId);
  }

  @PostMapping("/{jobDefId}")
  public ApiResponse<String> createJobConfig(
      @PathVariable("jobDefId") String jobDefId, @RequestBody JobConfigRequest request) {
    List<Step> steps =
        new ArrayList<>(
            2
                + (CollectionUtils.isEmpty(request.getTransforms())
                    ? 0
                    : request.getTransforms().size()));
    steps.add(DtoBoMapper.toStep(request.getSource()));
    if (CollectionUtils.isNotEmpty(request.getTransforms())) {
      List<Step> transforms =
          request.getTransforms().stream().map(DtoBoMapper::toStep).collect(Collectors.toList());
      steps.addAll(transforms);
    }
    steps.add(DtoBoMapper.toStep(request.getSink()));
    jobDefinitionService.updateJobConfig(jobDefId, steps);
    return ApiResponse.ok(jobDefId);
  }

  @PutMapping("/{jobName}/{version}")
  public ApiResponse<String> updateJobDefinition(
      @PathVariable("jobName") String jobName,
      @PathVariable("version") int version,
      @RequestBody UpdateJobDefinitionRequest request) {
    String jobId =
        jobDefinitionService.updateByVersion(
            jobName, version, request.getName(), request.getDescription(), null);
    return ApiResponse.ok(jobId);
  }

  @PutMapping("/{jobName}/{version}")
  public ApiResponse<String> updateJobConfig(
      @PathVariable("jobName") String jobName,
      @PathVariable("version") int version,
      @RequestBody UpdateJobConfigRequest request) {
    List<Step> steps =
        new ArrayList<>(
            2
                + (CollectionUtils.isEmpty(request.getTransforms())
                    ? 0
                    : request.getTransforms().size()));
    steps.add(DtoBoMapper.toStep(request.getSource()));
    if (CollectionUtils.isNotEmpty(request.getTransforms())) {
      List<Step> transforms =
          request.getTransforms().stream().map(DtoBoMapper::toStep).collect(Collectors.toList());
      steps.addAll(transforms);
    }
    steps.add(DtoBoMapper.toStep(request.getSink()));
    String jobId = jobDefinitionService.updateByVersion(jobName, version, null, null, steps);
    return ApiResponse.ok(jobId);
  }

  //  public ApiResponse<List<JobDefinitionVO>>
}
