package io.github.octopus.sql.executor.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Page<T> {

  private long total;
  private long pageNum;
  private long pageSize;
  private long pageCount;
  private List<T> records;
}