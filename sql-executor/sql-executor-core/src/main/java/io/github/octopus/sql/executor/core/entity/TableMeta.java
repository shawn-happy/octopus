package io.github.octopus.sql.executor.core.entity;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableMeta {
  private String database;
  private String schema;
  private String table;
  private long rowNumber;
  private long rowSize;
  private LocalDateTime createTime;
  private LocalDateTime updateTime;
  private String comment;

  // for mysql
  private String engine;
}
