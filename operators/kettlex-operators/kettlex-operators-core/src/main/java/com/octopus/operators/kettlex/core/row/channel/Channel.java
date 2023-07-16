package com.octopus.operators.kettlex.core.row.channel;

import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import java.util.Collection;

public interface Channel {

  String getId();

  void push(final Record r);

  void pushAll(final Collection<Record> rs);

  void pushTerminate(TerminateRecord tr);

  Record pull();

  int size();

  boolean isEmpty();

  void clear();
}
