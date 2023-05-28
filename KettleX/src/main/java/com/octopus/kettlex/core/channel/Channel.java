package com.octopus.kettlex.core.channel;

import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.record.TerminateRecord;
import java.util.Collection;

public interface Channel {

  void push(final Record r);

  void pushAll(final Collection<Record> rs);

  void pushTerminate(TerminateRecord tr);

  Record pull();

  int size();

  boolean isEmpty();

  void clear();
}
