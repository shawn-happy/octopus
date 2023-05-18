package com.octopus.kettlex.core.channel;

import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.statistics.Communication;
import java.util.Collection;
import java.util.List;

public interface Channel {

  Communication getCommunication();

  boolean put(Record record);

  boolean putAll(Collection<Record> records);

  Record pull();

  List<Record> pullAll();

  boolean isEmpty();

  int size();

  void clear();
}
