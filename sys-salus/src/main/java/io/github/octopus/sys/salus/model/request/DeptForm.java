package io.github.octopus.sys.salus.model.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeptForm {
  @Default private Long pid = -1L;
  private String code;
  private String name;
  private String leader;
  private String phone;
  private String email;
  private String remark;
  private Integer order;
}
