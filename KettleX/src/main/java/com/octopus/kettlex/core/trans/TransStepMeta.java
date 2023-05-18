package com.octopus.kettlex.core.trans;

import com.octopus.kettlex.core.plugin.PluginType;
import com.octopus.kettlex.core.steps.StepMeta;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransStepMeta {

  private String id;
  private String name;
  private String comment;
  @Default private final PluginType type = PluginType.STEP;
  private StepMeta stepMeta;
}
