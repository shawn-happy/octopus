package io.github.shawn.octopus.fluxus.engine.common.file;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.serialization.DeserializationSchema;
import io.github.shawn.octopus.fluxus.api.serialization.SerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.serialization.CSVSerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.serialization.CSVToRowRecordDeserialization;
import io.github.shawn.octopus.fluxus.engine.common.file.serialization.JsonSerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.serialization.JsonToRowRecordDeserialization;
import io.github.shawn.octopus.fluxus.engine.connector.sink.file.FileSinkConfig;
import io.github.shawn.octopus.fluxus.engine.connector.source.file.FileSourceConfig;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;

public enum FileFormat {
  CSV("CSV") {

    @Override
    public DeserializationSchema<RowRecord> createDeserialization(
        RowFieldType rowType, FileSourceConfig.FileSourceOptions options) {
      return new CSVToRowRecordDeserialization(
          rowType, (FileSourceConfig.CSVFileSourceOptions) options);
    }

    @Override
    public SerializationSchema createSerialization(
        RowFieldType rowType, FileSinkConfig.FileSinkOptions options) {
      return new CSVSerializationSchema(rowType, (FileSinkConfig.CSVFileSinkOptions) options);
    }
  },
  JSON("JSON") {

    @Override
    public DeserializationSchema<RowRecord> createDeserialization(
        RowFieldType rowType, FileSourceConfig.FileSourceOptions options) {
      return new JsonToRowRecordDeserialization(
          rowType, (FileSourceConfig.JsonFileSourceOptions) options);
    }

    @Override
    public SerializationSchema createSerialization(
        RowFieldType rowType, FileSinkConfig.FileSinkOptions options) {
      return new JsonSerializationSchema(rowType);
    }
  },
  PARQUET("PARQUET") {
    @Override
    public DeserializationSchema<RowRecord> createDeserialization(
        RowFieldType rowType, FileSourceConfig.FileSourceOptions options) {
      return null;
    }

    @Override
    public SerializationSchema createSerialization(
        RowFieldType rowType, FileSinkConfig.FileSinkOptions options) {
      return null;
    }
  },

  ORC("ORC") {
    @Override
    public DeserializationSchema<RowRecord> createDeserialization(
        RowFieldType rowType, FileSourceConfig.FileSourceOptions options) {
      return null;
    }

    @Override
    public SerializationSchema createSerialization(
        RowFieldType rowType, FileSinkConfig.FileSinkOptions options) {
      return null;
    }
  },
  ;

  FileFormat(String format) {
    this.format = format;
  }

  private final String format;

  public String format() {
    return format;
  }

  public abstract DeserializationSchema<RowRecord> createDeserialization(
      RowFieldType rowType, FileSourceConfig.FileSourceOptions options);

  public abstract SerializationSchema createSerialization(
      RowFieldType rowType, FileSinkConfig.FileSinkOptions options);
}
