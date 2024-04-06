package io.github.shawn.octopus.fluxus.engine.common.jdbc.connection;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JdbcConnectionConfig {
  private String url;
  private String username;
  private String password;
  private String driverClass;
  @Builder.Default private boolean autoCommit = false;
}
