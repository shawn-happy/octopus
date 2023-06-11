package com.octopus.kettlex.core.steps.config;

import com.fasterxml.jackson.core.Version;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskConfiguration {

  private String taskId;
  private String taskName;
  private Version Version;
  private String description;

  private List<ReaderConfig<?>> readers;
  private List<TransformationConfig<?>> transformations;
  private List<WriterConfig<?>> writers;
  private RuntimeConfig runtimeConfig;
}
