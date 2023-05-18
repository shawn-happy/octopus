package com.octopus.kettlex.core.row;

public enum FieldType {
  Boolean,
  Integer,
  Long,
  Float,
  Double,
  String,
  Date,
  DateTime,
  Timestamp,
  Array,
  ;

  public static boolean isNumber(FieldType type) {
    return Integer == type || Long == type || Float == type || Double == type;
  }
}
