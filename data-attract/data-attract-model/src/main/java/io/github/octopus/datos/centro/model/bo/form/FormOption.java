package io.github.octopus.datos.centro.model.bo.form;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FormOption {

  private String label;
  private String chName;
  private Object defaultValue;
  private String hint;
  private boolean required;
  private FormType formType;
}
