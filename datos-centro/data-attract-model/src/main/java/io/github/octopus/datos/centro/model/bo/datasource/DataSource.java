package io.github.octopus.datos.centro.model.bo.datasource;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataSource {

  private long id;
  private String code;
  private String name;
  private String description;
  private DataSourceType type;
  private DataSourceConfig config;
  private String creator;
  private String updater;
  private LocalDateTime createTime;
  private LocalDateTime updateTime;
}
