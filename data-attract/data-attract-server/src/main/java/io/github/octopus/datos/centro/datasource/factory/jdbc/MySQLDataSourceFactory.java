package io.github.octopus.datos.centro.datasource.factory.jdbc;

public class MySQLDataSourceFactory extends JdbcDataSourceFactory {

  @Override
  public String factoryIdentifier() {
    return "jdbc_mysql";
  }
}
