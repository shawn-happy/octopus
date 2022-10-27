package com.shawn.octopus.spark.etl.example;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class SparkSQLDataSourceDemo {

  @Test
  public void testJDBCReader() {

    int count = 1000;
    int numPartitions = 10;
    int stride = count / numPartitions;
    SparkSession spark =
        SparkSession.builder().appName("local-test").master("local[2]").getOrCreate();
    String[] predicates = new String[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      predicates[i] = String.format("1 = 1 limit %d, %d", i * stride, stride);
    }
    Properties properties = new Properties();
    properties.put("user", "root");
    properties.put("password", "Admin@4pd");
    properties.put("driver", "com.mysql.cj.jdbc.Driver");
    Dataset<Row> df =
        spark
            .read()
            .jdbc(
                "jdbc:mysql://172.27.231.44:3306/seal?createDatabaseIfNotExist=true&serverTimezone=UTC&useSSL=true",
                "data_import",
                predicates,
                properties);
    System.out.println(df.rdd().getNumPartitions());
    //    for (int i = 0; i < numPartitions; i++) {
    //      dfs.add(
    //          spark
    //              .read()
    //              .format("jdbc")
    //              .option(
    //                  "url",
    //
    // "jdbc:mysql://172.27.231.44:3306/seal?createDatabaseIfNotExist=true&serverTimezone=UTC&useSSL=true")
    //              .option("user", "root")
    //              .option("password", "Admin@4pd")
    //              .option("driver", "com.mysql.cj.jdbc.Driver")
    //              .option(
    //                  "dbtable",
    //                  String.format(
    //                      "(select * from data_import limit %d, %d) as subQuery", i * stride,
    // stride))
    //              .load());
    //    }
    //    Dataset<Row> df1 = dfs.get(0);
    //    for (int i = 1; i < dfs.size(); i++) {
    //      df1.union(dfs.get(i));
    //    }
    //
    //    df1.show(10);

    //    spark
    //        .read()
    //        .format("jdbc")
    //        .option(
    //            "url",
    //
    // "jdbc:mysql://172.27.231.44:3306/seal?createDatabaseIfNotExist=true&serverTimezone=UTC&useSSL=true")
    //        .option("user", "root")
    //        .option("password", "Admin@4pd")
    //        .option("driver", "com.mysql.cj.jdbc.Driver")
    //        .option("query", "select * from data_import")
    //        .load()
    //        .where("id > 10")
    //        .show(10);
    //    int numPartitions = df.rdd().getNumPartitions();
    //    df.show(10);
    //    System.out.println(numPartitions);
  }

  @Test
  public void testJDBCWriter() {
    SparkSession spark =
        SparkSession.builder().appName("local-test").master("local[2]").getOrCreate();
    StructType structType =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("name", DataTypes.StringType, false, Metadata.empty()),
              new StructField("createTime", DataTypes.TimestampType, false, Metadata.empty())
            });
    Dataset<Row> df =
        spark
            .read()
            .option("header", "true")
            .schema(structType)
            .format("csv")
            .load(SparkSQLDataSourceDemo.class.getClassLoader().getResource("user.csv").getPath());
    Properties properties = new Properties();
    properties.put("user", "root");
    properties.put("password", "Admin@4pd");
    properties.put("driver", "com.mysql.cj.jdbc.Driver");
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(
            "jdbc:mysql://172.27.231.44:3306/test?createDatabaseIfNotExist=true&serverTimezone=UTC&useSSL=true",
            "user2",
            properties);
  }
}
