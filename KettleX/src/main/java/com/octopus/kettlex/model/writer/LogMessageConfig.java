package com.octopus.kettlex.model.writer;

import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.model.WriteMode;
import com.octopus.kettlex.model.WriterConfig;
import com.octopus.kettlex.model.WriterOptions;
import com.octopus.kettlex.model.writer.LogMessageConfig.LogMessageOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogMessageConfig implements WriterConfig<LogMessageOptions> {

  private String name;
  @Default private final StepType type = StepType.LOG_MESSAGE;
  private String input;
  private WriteMode writeMode;
  private LogMessageOptions options;

  @Getter
  @Builder
  @NoArgsConstructor
  public static class LogMessageOptions implements WriterOptions {

    @Override
    public void verify() {}
  }
}
