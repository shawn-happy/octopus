package com.octopus.kettlex.core.steps.writer.log;

import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.steps.StepType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogMessageMeta implements StepMeta {

  private String id;
  private String name;
  @Default private final StepType stepType = StepType.LOG_MESSAGE;
}
