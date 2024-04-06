package io.github.shawn.octopus.fluxus.executor.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.github.shawn.octopus.fluxus.executor.bo.JobDefinitionInfo;
import io.github.shawn.octopus.fluxus.executor.bo.Step;

import java.util.List;

public interface JobDefinitionService {

  String createJobDefinition(String name, String description);

  JobDefinitionInfo findById(String jobDefId);

  void delete(String jobDefId);

  String updateByVersion(
      String jobName, int version, String newName, String description, List<Step> steps);

  void updateJobConfig(String jobDefId, List<Step> steps);

  Page<JobDefinitionInfo> findJobDefPage(String name, int pageNum, int pageSize);
}
