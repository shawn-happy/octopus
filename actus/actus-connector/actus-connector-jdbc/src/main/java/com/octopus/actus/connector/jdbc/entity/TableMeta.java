package com.octopus.actus.connector.jdbc.entity;

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
  private String tableSchema;
  private String tableName;
  private String engine;
  private long tableRows;
  private long dataLength;
  private long maxDataLength;
  private LocalDateTime createTime;
  private LocalDateTime updateTime;
  private String tableCollation;
  private String tableComment;
}
