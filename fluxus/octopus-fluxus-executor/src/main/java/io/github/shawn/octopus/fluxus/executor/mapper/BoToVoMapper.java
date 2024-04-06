package io.github.shawn.octopus.fluxus.executor.mapper;

import io.github.shawn.octopus.fluxus.executor.bo.JobDefinitionInfo;
import io.github.shawn.octopus.fluxus.executor.bo.Step;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.vo.JobDefinitionVO;
import io.github.shawn.octopus.fluxus.executor.vo.StepAttributeVO;
import io.github.shawn.octopus.fluxus.executor.vo.StepVO;

import java.util.Optional;
import java.util.stream.Collectors;

public class BoToVoMapper {

  public static JobDefinitionVO toJobDefVo(JobDefinitionInfo jobDef) {
    return Optional.ofNullable(jobDef)
        .map(
            def ->
                JobDefinitionVO.builder()
                    .jobId(def.getJobId())
                    .name(def.getName())
                    .description(def.getDescription())
                    .version(def.getVersion().toString())
                    .build())
        .orElse(null);
  }

  public static StepVO toStepVo(Step step) {
    return Optional.ofNullable(step)
        .map(
            e ->
                StepVO.builder()
                    .stepId(e.getId())
                    .stepName(e.getName())
                    .type(e.getPluginType().getType())
                    .identifier(e.getIdentify())
                    .description(e.getDescription())
                    .input(e.getInput())
                    .output(e.getOutput())
                    .attributes(
                        e.getAttributes()
                            .stream()
                            .map(BoToVoMapper::toStepAttributeVo)
                            .collect(Collectors.toList()))
                    .build())
        .orElse(null);
  }

  public static StepAttributeVO toStepAttributeVo(StepAttribute attribute) {
    return Optional.ofNullable(attribute)
        .map(
            e ->
                StepAttributeVO.builder()
                    .attributeId(e.getId())
                    .code(e.getCode())
                    .value(e.getValue())
                    .build())
        .orElse(null);
  }
}
