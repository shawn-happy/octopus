package io.github.octopus.actus.core.model.metadata;

import io.github.octopus.actus.core.model.schema.TableEngine;
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
  private String schemaName;
  private String tableName;
  private String comment;
  private long recordSize;
  private long recordNumber;
  private LocalDateTime createTime;
  private LocalDateTime updateTime;

  // for mysql
  private TableEngine engine;
}
