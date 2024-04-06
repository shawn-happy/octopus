package io.github.shawn.octopus.fluxus.engine.common.jdbc.connection;

import io.github.shawn.octopus.fluxus.api.common.PredicateUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class DefaultJdbcConnectionProvider implements JdbcConnectionProvider {
  private final JdbcConnectionConfig jdbcConfig;

  private transient Driver loadedDriver;
  private transient Connection connection;

  public DefaultJdbcConnectionProvider(@NotNull JdbcConnectionConfig jdbcConfig) {
    this.jdbcConfig = jdbcConfig;
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean isConnectionValid() throws SQLException {
    return connection != null && !connection.isClosed();
  }

  private static Driver loadDriver(String driverName) throws ClassNotFoundException {
    PredicateUtils.verify(StringUtils.isNotBlank(driverName), "driver class name cannot be null");
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getName().equals(driverName)) {
        return driver;
      }
    }

    // We could reach here for reasons:
    // * Class loader hell of DriverManager(see JDK-8146872).
    // * driver is not installed as a service provider.
    Class<?> clazz =
        Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
    try {
      return (Driver) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception ex) {
      throw new DataWorkflowException("Fail to create driver of class " + driverName, ex);
    }
  }

  private Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
    if (loadedDriver == null) {
      loadedDriver = loadDriver(jdbcConfig.getDriverClass());
    }
    return loadedDriver;
  }

  @Override
  public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
    if (isConnectionValid()) {
      return connection;
    }
    Driver driver = getLoadedDriver();
    Properties info = new Properties();
    if (StringUtils.isNotBlank(jdbcConfig.getUsername())) {
      info.setProperty("user", jdbcConfig.getUsername());
    }
    if (StringUtils.isNotBlank(jdbcConfig.getPassword())) {
      info.setProperty("password", jdbcConfig.getPassword());
    }
    connection = driver.connect(jdbcConfig.getUrl(), info);
    if (connection == null) {
      // Throw same exception as DriverManager.getConnection when no driver found to match
      // caller expectation.
      throw new DataWorkflowException("No suitable driver found for " + jdbcConfig.getUrl());
    }

    connection.setAutoCommit(jdbcConfig.isAutoCommit());

    return connection;
  }

  @Override
  public void closeConnection() {
    try {
      if (isConnectionValid()) {
        connection.close();
      }
    } catch (SQLException e) {
      log.warn("JDBC connection close failed.", e);
    } finally {
      connection = null;
    }
  }

  @Override
  public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
    closeConnection();
    return getOrEstablishConnection();
  }
}
