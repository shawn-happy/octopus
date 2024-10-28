package io.github.octopus.datos.centro.model.bo.form;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FormStructure {
  private String name;
  private List<FormOption> options;
}
