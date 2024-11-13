package io.github.octopus.sql.executor.plugin.mysql.executor;

import com.alibaba.druid.pool.DruidDataSource;
import io.github.octopus.sql.executor.core.model.schema.DatabaseDefinition;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MySQLDDLExecutorTests {

  @ClassRule
  public static MySQLContainer<?> mySQLContainer =
      new MySQLContainer<>("mysql:5.7").withUsername("root").withPassword("root");

  private static DruidDataSource dataSource;
  private static MySQLDDLExecutor mySQLDDLExecutor;

  @BeforeAll
  public static void setUp() {
    mySQLContainer.start();
    String jdbcUrl = mySQLContainer.getJdbcUrl();
    String username = mySQLContainer.getUsername();
    String password = mySQLContainer.getPassword();
    String driver = mySQLContainer.getDriverClassName();

    dataSource = new DruidDataSource();
    dataSource.setUrl(jdbcUrl);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(driver);
    mySQLDDLExecutor = new MySQLDDLExecutor(dataSource);
  }

  @AfterAll
  public static void close() {
    dataSource.close();
    mySQLContainer.close();
  }

  @Test
  public void testCreateDatabase() throws Exception {
    mySQLDDLExecutor.createDatabase(DatabaseDefinition.builder().database("test_shawn").build());
    PreparedStatement ps = dataSource.getConnection().prepareStatement("show databases");
    ResultSet resultSet = ps.executeQuery();
    while (resultSet.next()) {
      String database = resultSet.getString(1);
      System.out.println(database);
    }
    resultSet.close();
    ps.close();
  }
}
