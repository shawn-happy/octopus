package io.github.octopus.datos.centro.sql.model;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableMetaInfo {
  private String databaseName;
  private String tableName;
  private String comment;
  private TableEngine engine;
  private long recordSize;
  private long dataLength;
  private long maxDataLength;
  private LocalDateTime createTime;
  private LocalDateTime updateTime;
  private String collation;
}
