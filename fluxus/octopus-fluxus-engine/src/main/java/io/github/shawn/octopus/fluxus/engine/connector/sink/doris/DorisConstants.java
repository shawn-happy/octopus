package io.github.shawn.octopus.fluxus.engine.connector.sink.doris;

public interface DorisConstants {

  String FIELD_DELIMITER_KEY = "column_separator";
  String LINE_DELIMITER_KEY = "line_delimiter";
  String LINE_DELIMITER_DEFAULT = "\n";
  String FORMAT_KEY = "format";
  String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
}
