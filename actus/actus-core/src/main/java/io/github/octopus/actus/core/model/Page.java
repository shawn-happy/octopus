package io.github.octopus.actus.core.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

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
