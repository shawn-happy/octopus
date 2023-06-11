package com.octopus.kettlex.steps;

import com.octopus.kettlex.core.steps.config.WriteMode;
import com.octopus.kettlex.core.steps.config.WriterConfig;
import com.octopus.kettlex.steps.LogMessageConfig.LogMessageOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogMessageConfig implements WriterConfig<LogMessageOptions> {

  private String id;
  private String name;
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
