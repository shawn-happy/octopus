package io.github.octopus.sys.salus.model.bo;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
  private Long id;
  private Long pid;
  private String code;
  private String name;
  private String ancestors;
  private String leaderName;
  private String leaderPhone;
  private String leaderEmail;
  private Integer orderNum;
  private String description;
  private boolean enabled;

  @Setter private List<Dept> childrenDept;
}
