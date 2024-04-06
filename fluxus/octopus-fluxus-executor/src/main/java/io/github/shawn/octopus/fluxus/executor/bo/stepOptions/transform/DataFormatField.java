package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataFormatField {
  private String destination;
  private String sourcePath;
  private String type;
}
