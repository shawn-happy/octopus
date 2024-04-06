package io.github.shawn.octopus.fluxus.executor.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.github.shawn.octopus.fluxus.executor.bo.JobDefinitionInfo;
import io.github.shawn.octopus.fluxus.executor.bo.Step;
import io.github.shawn.octopus.fluxus.executor.bo.Version;
import io.github.shawn.octopus.fluxus.executor.dao.JobDao;
import io.github.shawn.octopus.fluxus.executor.dao.JobVersionDao;
import io.github.shawn.octopus.fluxus.executor.dao.StepAttributeDao;
import io.github.shawn.octopus.fluxus.executor.dao.StepDao;
import io.github.shawn.octopus.fluxus.executor.entity.JobEntity;
import io.github.shawn.octopus.fluxus.executor.entity.JobVersionEntity;
import io.github.shawn.octopus.fluxus.executor.entity.StepAttributeEntity;
import io.github.shawn.octopus.fluxus.executor.entity.StepEntity;
import io.github.shawn.octopus.fluxus.executor.exception.DataWorkflowWebException;
import io.github.shawn.octopus.fluxus.executor.mapper.EntityToBoMapper;
import io.github.shawn.octopus.fluxus.executor.service.JobDefinitionService;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobDefinitionServiceImpl implements JobDefinitionService {

  private final JobDao jobDao;
  private final JobVersionDao jobVersionDao;
  private final StepDao stepDao;
  private final StepAttributeDao attributeDao;

  public JobDefinitionServiceImpl(
      JobDao jobDao, JobVersionDao jobVersionDao, StepDao stepDao, StepAttributeDao attributeDao) {
    this.jobDao = jobDao;
    this.jobVersionDao = jobVersionDao;
    this.stepDao = stepDao;
    this.attributeDao = attributeDao;
  }

  @Override
  @Transactional
  public String createJobDefinition(String name, String description) {
    List<JobEntity> jobEntities = jobDao.selectByName(name);
    if (CollectionUtils.isNotEmpty(jobEntities)) {
      throw new DataWorkflowWebException(String.format("The job definition %s is exist", name));
    }
    JobEntity jobEntity = EntityToBoMapper.createJob(name, description);
    List<JobVersionEntity> jobVersionEntities = jobEntity.getJobVersionEntities();
    JobVersionEntity jobVersionEntity = jobVersionEntities.get(0);
    jobVersionDao.insert(jobVersionEntity);
    jobDao.insert(jobEntity);
    return jobEntity.getId();
  }

  @Override
  public JobDefinitionInfo findById(String jobDefId) {
    JobEntity jobEntity = jobDao.selectById(jobDefId);
    if (jobEntity == null) {
      return null;
    }

    List<JobVersionEntity> jobVersionEntities =
        jobVersionDao.selectList(
            Wrappers.lambdaQuery(JobVersionEntity.class).eq(JobVersionEntity::getJobId, jobDefId));
    if (CollectionUtils.isEmpty(jobVersionEntities)) {
      throw new DataWorkflowWebException(
          "job version cannot be null, job definition id is: " + jobDefId);
    }
    JobVersionEntity jobVersionEntity =
        jobVersionEntities.stream()
            .max(Comparator.comparing(JobVersionEntity::getVersion))
            .orElseThrow(
                () ->
                    new DataWorkflowWebException(
                        "job version cannot be null, job definition id is: " + jobDefId));
    return EntityToBoMapper.toJobDefinition(jobEntity, Version.of(jobVersionEntity.getVersion()));
  }

  @Override
  @Transactional
  public void delete(String jobDefId) {
    attributeDao.delete(
        Wrappers.lambdaQuery(StepAttributeEntity.class)
            .eq(StepAttributeEntity::getJobId, jobDefId));
    stepDao.delete(Wrappers.lambdaQuery(StepEntity.class).eq(StepEntity::getJobId, jobDefId));
    jobVersionDao.deleteByJobDefId(jobDefId);
    jobDao.deleteById(jobDefId);
  }

  @Override
  @Transactional
  public String updateByVersion(
      String jobName, int version, String newName, String description, List<Step> steps) {
    JobVersionEntity jobVersionEntity = jobVersionDao.selectOneByNameAndVersion(jobName, version);
    if (jobVersionEntity == null) {
      throw new DataWorkflowWebException(
          String.format("job version not found by name: %s, version: %d", jobName, version));
    }
    String jobId = jobVersionEntity.getJobId();
    JobEntity jobEntity = jobDao.selectById(jobId);
    long now = System.currentTimeMillis();
    if (jobEntity == null) {
      throw new DataWorkflowWebException(String.format("job not found by id: %s", jobId));
    }
    if (!jobEntity.getName().equals(newName) || !jobEntity.getDescription().equals(description)) {
      if (StringUtils.isNotBlank(newName)) {
        jobEntity.setName(newName);
        jobVersionEntity.setJobName(newName);
      }
      if (StringUtils.isNotBlank(jobEntity.getDescription())
          && StringUtils.isNotBlank(description)) {
        jobEntity.setDescription(description);
      }
      jobEntity.setUpdateTime(now);
      jobDao.updateById(jobEntity);
    }
    if (CollectionUtils.isNotEmpty(steps)) {
      List<StepEntity> stepEntities =
          steps.stream()
              .map(step -> EntityToBoMapper.createStepEntity(step, jobId, now))
              .collect(Collectors.toList());
      List<StepAttributeEntity> stepAttributeEntities =
          stepEntities.stream()
              .map(StepEntity::getStepAttributeEntities)
              .flatMap(Collection::stream)
              .collect(Collectors.toList());
      List<String> stepIds = jobVersionEntity.getSteps();
      attributeDao.delete(
          Wrappers.lambdaQuery(StepAttributeEntity.class)
              .in(StepAttributeEntity::getStepId, stepIds));
      stepDao.delete(Wrappers.lambdaQuery(StepEntity.class).in(StepEntity::getId, stepIds));
      attributeDao.insertBatch(stepAttributeEntities);
      stepDao.insertBatch(stepEntities);
      List<String> newStepIds =
          stepEntities.stream().map(StepEntity::getId).collect(Collectors.toList());
      jobVersionEntity.setSteps(newStepIds);
    }

    jobVersionEntity.setUpdateTime(now);
    jobVersionDao.updateById(jobVersionEntity);
    return jobId;
  }

  @Override
  @Transactional
  public void updateJobConfig(String jobDefId, List<Step> steps) {
    JobEntity jobEntity = jobDao.selectById(jobDefId);
    if (jobEntity == null) {
      throw new DataWorkflowWebException("job cannot found, job definition id is: " + jobDefId);
    }
    List<JobVersionEntity> jobVersionEntities =
        jobVersionDao.selectList(
            Wrappers.lambdaQuery(JobVersionEntity.class).eq(JobVersionEntity::getJobId, jobDefId));
    if (CollectionUtils.isEmpty(jobVersionEntities)) {
      throw new DataWorkflowWebException(
          "job version cannot be null, job definition id is: " + jobDefId);
    }
    JobVersionEntity jobVersionEntity =
        jobVersionEntities.stream()
            .max(Comparator.comparing(JobVersionEntity::getVersion))
            .orElseThrow(
                () ->
                    new DataWorkflowWebException(
                        "job version cannot be null, job definition id is: " + jobDefId));
    Version version = Version.of(jobVersionEntity.getVersion());

    long now = System.currentTimeMillis();
    List<StepEntity> stepEntities =
        steps.stream()
            .map(step -> EntityToBoMapper.createStepEntity(step, jobDefId, now))
            .collect(Collectors.toList());
    List<StepAttributeEntity> stepAttributeEntities =
        stepEntities.stream()
            .map(StepEntity::getStepAttributeEntities)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    version.next();
    jobVersionEntity.setVersion(version.getCurrentVersion());
    jobVersionEntity.setCreateTime(now);
    jobVersionEntity.setUpdateTime(now);
    jobVersionEntity.setStepEntities(stepEntities);
    jobVersionEntity.setSteps(
        stepEntities.stream().map(StepEntity::getId).collect(Collectors.toList()));

    jobEntity.setUpdateTime(now);

    attributeDao.insertBatch(stepAttributeEntities);
    stepDao.insertBatch(stepEntities);
    jobVersionDao.insert(jobVersionEntity);
    jobDao.updateById(jobEntity);
  }

  @Override
  public Page<JobDefinitionInfo> findJobDefPage(String name, int pageNum, int pageSize) {
    Page<JobEntity> jobEntityPage =
        jobDao.selectPage(Page.of(pageNum, pageSize), Wrappers.lambdaQuery(JobEntity.class));
    return null;
  }
}
