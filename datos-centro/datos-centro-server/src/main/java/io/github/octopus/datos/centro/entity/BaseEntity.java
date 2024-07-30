package io.github.octopus.datos.centro.entity;

import java.time.LocalDateTime;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BaseEntity {

  private String creator;
  private String updater;
  private LocalDateTime createTime;
  private LocalDateTime updateTime;

}
