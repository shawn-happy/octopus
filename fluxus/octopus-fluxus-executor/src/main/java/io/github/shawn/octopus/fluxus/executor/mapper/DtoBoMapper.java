package io.github.shawn.octopus.fluxus.executor.mapper;

import io.github.shawn.octopus.fluxus.executor.bo.JobDefinitionInfo;
import io.github.shawn.octopus.fluxus.executor.bo.Step;
import io.github.shawn.octopus.fluxus.executor.dto.JobDefinitionRequest;
import io.github.shawn.octopus.fluxus.executor.dto.StepRequest;

import java.util.Optional;

public class DtoBoMapper {
  public static JobDefinitionInfo toJobDefinition(JobDefinitionRequest jobDefReq) {
    return Optional.ofNullable(jobDefReq)
        .map(
            request ->
                JobDefinitionInfo.builder()
                    .name(request.getJobName())
                    .description(request.getDescription())
                    .build())
        .orElse(null);
  }

  public static Step toStep(StepRequest stepRequest) {
    return Optional.ofNullable(stepRequest)
        .map(
            request ->
                Step.builder()
                    .name(request.getName())
                    .pluginType(request.getOptions().getPluginType())
                    .identify(request.getOptions().getIdentify())
                    .input(request.getInput())
                    .output(request.getOutput())
                    .attributes(request.getOptions().getStepAttributes())
                    .build())
        .orElse(null);
  }
}
