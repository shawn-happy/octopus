package com.octopus.kettlex.core.row.record;

import com.octopus.kettlex.core.channel.Channel;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.RecordExchanger;

public class DefaultRecordExchanger implements RecordExchanger {

  private volatile boolean shutdown = false;
  private final Channel channel;

  public DefaultRecordExchanger(Channel channel) {
    this.channel = channel;
  }

  @Override
  public void send(Record record) {
    if (shutdown) {
      throw new KettleXException();
    }
    if (record == null) {
      return;
    }
    this.channel.push(record);
  }

  @Override
  public Record fetch() {
    if (shutdown) {
      throw new KettleXException();
    }
    Record record = this.channel.pull();
    return (record instanceof TerminateRecord ? null : record);
  }

  @Override
  public void flush() {}

  @Override
  public void terminate() {
    if (shutdown) {
      throw new KettleXException();
    }
    this.channel.pushTerminate(TerminateRecord.get());
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }
}
