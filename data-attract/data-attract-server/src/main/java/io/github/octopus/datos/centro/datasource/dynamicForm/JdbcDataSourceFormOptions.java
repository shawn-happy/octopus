package io.github.octopus.datos.centro.datasource.dynamicForm;

import io.github.octopus.datos.centro.model.bo.form.FormOption;
import io.github.octopus.datos.centro.model.bo.form.FormOptions;
import io.github.octopus.datos.centro.model.bo.form.FormType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public enum JdbcDataSourceFormOptions implements FormOptions {
  URL(
      FormOption.builder()
          .label("url")
          .chName("数据库连接")
          .formType(FormType.TEXT)
          .hint("数据库jdbc连接url")
          .required(true)
          .defaultValue(null)
          .build()),

  USERNAME(
      FormOption.builder()
          .label("username")
          .chName("用户名")
          .formType(FormType.TEXT)
          .hint("数据库jdbc连接用户名")
          .required(true)
          .defaultValue(null)
          .build()),

  PASSWORD(
      FormOption.builder()
          .label("password")
          .chName("密码")
          .formType(FormType.PASSWORD)
          .hint("数据库jdbc连接密码")
          .required(false)
          .defaultValue(null)
          .build()),

  DRIVER_CLASS_NAME(
      FormOption.builder()
          .label("driverClassName")
          .chName("驱动包")
          .formType(FormType.TEXT)
          .hint("数据库jdbc连接驱动")
          .required(true)
          .defaultValue(null)
          .build()),

  DATABASE(
      FormOption.builder()
          .label("database")
          .chName("数据库")
          .formType(FormType.TEXT)
          .hint("数据库")
          .required(false)
          .defaultValue(null)
          .build()),
  ;

  private final FormOption formOption;

  JdbcDataSourceFormOptions(FormOption formOption) {
    this.formOption = formOption;
  }

  public static List<FormOption> getFormOptions() {
    return Arrays.stream(values())
        .map(JdbcDataSourceFormOptions::getFormOption)
        .collect(Collectors.toUnmodifiableList());
  }
}
