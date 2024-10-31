package io.github.octopus.sql.executor.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ParameterMapping {
    private int i;
    private String param;
    private Object value;

}
