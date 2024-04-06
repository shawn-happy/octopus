package io.github.shawn.octopus.fluxus.executor.mapper;

import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.JobDefinitionInfo;
import io.github.shawn.octopus.fluxus.executor.bo.Step;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.bo.Version;
import io.github.shawn.octopus.fluxus.executor.entity.JobEntity;
import io.github.shawn.octopus.fluxus.executor.entity.JobVersionEntity;
import io.github.shawn.octopus.fluxus.executor.entity.StepAttributeEntity;
import io.github.shawn.octopus.fluxus.executor.entity.StepEntity;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class EntityToBoMapper {

  public static JobEntity createJob(String jobName, String description) {
    long now = System.currentTimeMillis();
    JobEntity jobEntity = new JobEntity();
    String jobId = IdGenerator.uuid();
    jobEntity.setId(jobId);
    jobEntity.setName(jobName);
    jobEntity.setDescription(description);
    jobEntity.setCreateTime(now);
    jobEntity.setUpdateTime(now);
    jobEntity.setJobVersionEntities(
        Collections.singletonList(createJobVersion(jobId, jobName, now)));
    return jobEntity;
  }

  private static JobVersionEntity createJobVersion(String jobId, String jobName, long now) {
    JobVersionEntity jobVersionEntity = new JobVersionEntity();
    jobVersionEntity.setJobId(jobId);
    jobVersionEntity.setJobName(jobName);
    jobVersionEntity.setCreateTime(now);
    jobVersionEntity.setUpdateTime(now);
    jobVersionEntity.setVersion(Version.getInitialVersion().getCurrentVersion());
    return jobVersionEntity;
  }

  private static JobVersionEntity createJobVersion(
      String jobId, String jobName, List<Step> steps, long now) {
    List<StepEntity> stepEntities =
        steps.stream().map(step -> createStepEntity(step, jobId, now)).collect(Collectors.toList());
    JobVersionEntity jobVersionEntity = new JobVersionEntity();
    jobVersionEntity.setJobId(jobId);
    jobVersionEntity.setJobName(jobName);
    jobVersionEntity.setCreateTime(now);
    jobVersionEntity.setUpdateTime(now);
    jobVersionEntity.setStepEntities(stepEntities);
    jobVersionEntity.setSteps(
        stepEntities.stream().map(StepEntity::getId).collect(Collectors.toList()));
    jobVersionEntity.setVersion(Version.getInitialVersion().getCurrentVersion());
    return jobVersionEntity;
  }

  public static StepEntity createStepEntity(Step step, String jobId, long now) {
    if (step == null) {
      return null;
    }
    StepEntity stepEntity = new StepEntity();
    String stepId = IdGenerator.uuid();
    stepEntity.setId(stepId);
    stepEntity.setName(step.getName());
    stepEntity.setDescription(step.getDescription());
    stepEntity.setType(step.getPluginType().getType());
    stepEntity.setIdentify(step.getIdentify());
    stepEntity.setJobId(jobId);
    stepEntity.setStepAttributeEntities(
        step.getAttributes().stream()
            .map(attribute -> createStepAttributeEntity(attribute, jobId, stepId, now))
            .collect(Collectors.toList()));
    stepEntity.setStepAttributes(
        stepEntity.getStepAttributeEntities().stream()
            .map(StepAttributeEntity::getId)
            .collect(Collectors.toList()));
    stepEntity.setInput(step.getInput());
    stepEntity.setOutput(step.getOutput());
    stepEntity.setCreateTime(now);
    stepEntity.setUpdateTime(now);
    return stepEntity;
  }

  private static StepAttributeEntity createStepAttributeEntity(
      StepAttribute stepAttribute, String jobId, String stepId, long now) {
    if (stepAttribute == null) {
      return null;
    }
    StepAttributeEntity attributeEntity = new StepAttributeEntity();
    attributeEntity.setId(IdGenerator.uuid());
    attributeEntity.setJobId(jobId);
    attributeEntity.setStepId(stepId);
    attributeEntity.setCode(stepAttribute.getCode());
    attributeEntity.setValue(String.valueOf(stepAttribute.getValue()));
    attributeEntity.setCreateTime(now);
    attributeEntity.setUpdateTime(now);
    return attributeEntity;
  }

  public static JobDefinitionInfo toJobDefinition(JobEntity jobEntity, Version lastVersion) {
    return Optional.ofNullable(jobEntity)
        .map(
            entity ->
                JobDefinitionInfo.builder()
                    .jobId(entity.getId())
                    .name(entity.getName())
                    .description(entity.getDescription())
                    .version(lastVersion)
                    .createTime(entity.getCreateTime())
                    .updateTime(entity.getUpdateTime())
                    .build())
        .orElse(null);
  }

  public static Step toStep(
      StepEntity stepEntity, List<StepAttributeEntity> stepAttributeEntities) {
    return Optional.ofNullable(stepEntity)
        .map(
            entity ->
                Step.builder()
                    .id(entity.getId())
                    .jobId(entity.getJobId())
                    .name(entity.getName())
                    .pluginType(PluginType.of(stepEntity.getType()))
                    .identify(entity.getIdentify())
                    .input(entity.getInput())
                    .output(entity.getOutput())
                    .attributes(
                        stepAttributeEntities.stream()
                            .map(EntityToBoMapper::toStepAttribute)
                            .collect(Collectors.toList()))
                    .createTime(entity.getCreateTime())
                    .updateTime(entity.getUpdateTime())
                    .build())
        .orElse(null);
  }

  public static StepAttribute toStepAttribute(StepAttributeEntity stepAttributeEntity) {
    return Optional.ofNullable(stepAttributeEntity)
        .map(
            entity ->
                StepAttribute.builder()
                    .id(entity.getId())
                    .jobId(entity.getJobId())
                    .stepId(entity.getStepId())
                    .code(entity.getCode())
                    .value(entity.getValue())
                    .createTime(entity.getCreateTime())
                    .updateTime(entity.getUpdateTime())
                    .build())
        .orElse(null);
  }
}
