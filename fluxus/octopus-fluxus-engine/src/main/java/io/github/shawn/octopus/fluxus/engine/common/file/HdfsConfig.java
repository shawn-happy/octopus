package io.github.shawn.octopus.fluxus.engine.common.file;

// import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
// import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
// import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

@Setter
@Getter
@NoArgsConstructor
@Slf4j
public class HdfsConfig {

  public static String AVRO_REQUESTED_PROJECTION = "parquet.avro.projection";
  public static final String AVRO_READ_SCHEMA = "parquet.avro.read.schema";
  public static final String AVRO_SCHEMA_METADATA_KEY = "parquet.avro.schema";
  public static final String OLD_AVRO_SCHEMA_METADATA_KEY = "avro.schema";
  public static final String AVRO_READ_SCHEMA_METADATA_KEY = "avro.read.schema";
  public static String AVRO_DATA_SUPPLIER = "parquet.avro.data.supplier";
  public static final String AVRO_COMPATIBILITY = "parquet.avro.compatible";
  public static final boolean AVRO_DEFAULT_COMPATIBILITY = true;
  public static final String READ_INT96_AS_FIXED = "parquet.avro.readInt96AsFixed";
  public static final boolean READ_INT96_AS_FIXED_DEFAULT = false;

  private Map<String, String> extraOptions = new HashMap<>();

  private FileSystemType type;
  private String hdfsNameKey;
  private String hdfsSitePath;
  private String kerberosPrincipal;
  private String kerberosKeytabPath;

  public String getHdfsNameKey() {
    if (type == FileSystemType.LOCAL) {
      return CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
    }
    return this.hdfsNameKey;
  }

  public HdfsConfig(String hdfsNameKey) {
    if (type == FileSystemType.LOCAL) {
      this.hdfsNameKey = CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
    } else {
      this.hdfsNameKey = hdfsNameKey;
    }
  }

  public void setExtraOptionsForConfiguration(Configuration configuration) {
    if (!extraOptions.isEmpty()) {
      extraOptions.forEach(configuration::set);
    }
    if (hdfsSitePath != null) {
      configuration.addResource(new Path(hdfsSitePath));
    }
  }

  public Configuration getConfiguration(String schema, String hdfsImpl) {
    Configuration configuration = new Configuration();
    configuration.setBoolean(READ_INT96_AS_FIXED, true);
    //    configuration.setBoolean(WRITE_FIXED_AS_INT96, true);
    //    configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
    //    configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, true);
    configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, this.getHdfsNameKey());
    configuration.set(String.format("fs.%s.impl", schema), hdfsImpl);
    this.setExtraOptionsForConfiguration(configuration);
    String principal = this.getKerberosPrincipal();
    String keytabPath = this.getKerberosKeytabPath();
    doKerberosAuthentication(configuration, principal, keytabPath);
    return configuration;
  }

  private static void doKerberosAuthentication(
      Configuration configuration, String principal, String keytabPath) {
    if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytabPath)) {
      log.warn(
          "Principal [{}] or keytabPath [{}] is empty, it will skip kerberos authentication",
          principal,
          keytabPath);
    } else {
      configuration.set("hadoop.security.authentication", "kerberos");
      UserGroupInformation.setConfiguration(configuration);
      try {
        log.info(
            "Start Kerberos authentication using principal {} and keytab {}",
            principal,
            keytabPath);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        log.info("Kerberos authentication successful");
      } catch (IOException e) {
        String errorMsg =
            String.format(
                "Kerberos authentication failed using this "
                    + "principal [%s] and keytab path [%s]",
                principal, keytabPath);
        throw new DataWorkflowException(errorMsg, e);
      }
    }
  }
}
