package com.octopus.kettlex.core.row;

public interface RecordExchanger {

  void send(Record record);

  Record fetch();

  void flush();

  void terminate();

  void shutdown();
}
