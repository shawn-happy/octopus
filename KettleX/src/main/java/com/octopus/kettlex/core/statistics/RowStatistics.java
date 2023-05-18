package com.octopus.kettlex.core.statistics;

public class RowStatistics {

  private long linesRead;
  private long linesWritten;
  private long linesUpdated;
  private long linesSkipped;
  private long linesRejected;
  private long linesInput;
  private long linesOutput;

  private final Object statusCountersLock = new Object();

  public RowStatistics() {
    synchronized (statusCountersLock) {
      linesRead = 0L; // new AtomicLong(0L); // Keep some statistics!
      linesWritten = 0L; // new AtomicLong(0L);
      linesUpdated = 0L; // new AtomicLong(0L);
      linesSkipped = 0L; // new AtomicLong(0L);
      linesRejected = 0L; // new AtomicLong(0L);
      linesInput = 0L; // new AtomicLong(0L);
      linesOutput = 0L; // new AtomicLong(0L);
    }
  }

  public long getLinesRead() {
    synchronized (statusCountersLock) {
      return linesRead;
    }
  }

  /**
   * Increments the number of lines read from previous steps by one
   *
   * @return Returns the new value
   */
  public long incrementLinesRead() {
    synchronized (statusCountersLock) {
      return ++linesRead;
    }
  }

  /**
   * Decrements the number of lines read from previous steps by one
   *
   * @return Returns the new value
   */
  public long decrementLinesRead() {
    synchronized (statusCountersLock) {
      return --linesRead;
    }
  }

  /** @param newLinesReadValue the new number of lines read from previous steps */
  public void setLinesRead(long newLinesReadValue) {
    synchronized (statusCountersLock) {
      linesRead = newLinesReadValue;
    }
  }

  /** @return Returns the number of lines read from an input source: database, file, socket, etc. */
  public long getLinesInput() {
    synchronized (statusCountersLock) {
      return linesInput;
    }
  }

  /**
   * Increments the number of lines read from an input source: database, file, socket, etc.
   *
   * @return the new incremented value
   */
  public long incrementLinesInput() {
    synchronized (statusCountersLock) {
      return ++linesInput;
    }
  }

  /**
   * @param newLinesInputValue the new number of lines read from an input source: database, file,
   *     socket, etc.
   */
  public void setLinesInput(long newLinesInputValue) {
    synchronized (statusCountersLock) {
      linesInput = newLinesInputValue;
    }
  }

  /**
   * @return Returns the number of lines written to an output target: database, file, socket, etc.
   */
  public long getLinesOutput() {
    synchronized (statusCountersLock) {
      return linesOutput;
    }
  }

  /**
   * Increments the number of lines written to an output target: database, file, socket, etc.
   *
   * @return the new incremented value
   */
  public long incrementLinesOutput() {
    synchronized (statusCountersLock) {
      return ++linesOutput;
    }
  }

  /**
   * @param newLinesOutputValue the new number of lines written to an output target: database, file,
   *     socket, etc.
   */
  public void setLinesOutput(long newLinesOutputValue) {
    synchronized (statusCountersLock) {
      linesOutput = newLinesOutputValue;
    }
  }

  /** @return Returns the linesWritten. */
  public long getLinesWritten() {
    synchronized (statusCountersLock) {
      return linesWritten;
    }
  }

  /**
   * Increments the number of lines written to next steps by one
   *
   * @return Returns the new value
   */
  public long incrementLinesWritten() {
    synchronized (statusCountersLock) {
      return ++linesWritten;
    }
  }

  /**
   * Decrements the number of lines written to next steps by one
   *
   * @return Returns the new value
   */
  public long decrementLinesWritten() {
    synchronized (statusCountersLock) {
      return --linesWritten;
    }
  }

  /** @param newLinesWrittenValue the new number of lines written to next steps */
  public void setLinesWritten(long newLinesWrittenValue) {
    synchronized (statusCountersLock) {
      linesWritten = newLinesWrittenValue;
    }
  }

  /**
   * @return Returns the number of lines updated in an output target: database, file, socket, etc.
   */
  public long getLinesUpdated() {
    synchronized (statusCountersLock) {
      return linesUpdated;
    }
  }

  /**
   * Increments the number of lines updated in an output target: database, file, socket, etc.
   *
   * @return the new incremented value
   */
  public long incrementLinesUpdated() {
    synchronized (statusCountersLock) {
      return ++linesUpdated;
    }
  }

  /**
   * @param newLinesUpdatedValue the new number of lines updated in an output target: database,
   *     file, socket, etc.
   */
  public void setLinesUpdated(long newLinesUpdatedValue) {
    synchronized (statusCountersLock) {
      linesUpdated = newLinesUpdatedValue;
    }
  }

  /** @return the number of lines rejected to an error handling step */
  public long getLinesRejected() {
    synchronized (statusCountersLock) {
      return linesRejected;
    }
  }

  /**
   * Increments the number of lines rejected to an error handling step
   *
   * @return the new incremented value
   */
  public long incrementLinesRejected() {
    synchronized (statusCountersLock) {
      return ++linesRejected;
    }
  }

  /** @param newLinesRejectedValue lines number of lines rejected to an error handling step */
  public void setLinesRejected(long newLinesRejectedValue) {
    synchronized (statusCountersLock) {
      linesRejected = newLinesRejectedValue;
    }
  }

  /** @return the number of lines skipped */
  public long getLinesSkipped() {
    synchronized (statusCountersLock) {
      return linesSkipped;
    }
  }

  /**
   * Increments the number of lines skipped
   *
   * @return the new incremented value
   */
  public long incrementLinesSkipped() {
    synchronized (statusCountersLock) {
      return ++linesSkipped;
    }
  }

  /** @param newLinesSkippedValue lines number of lines skipped */
  public void setLinesSkipped(long newLinesSkippedValue) {
    synchronized (statusCountersLock) {
      linesSkipped = newLinesSkippedValue;
    }
  }
}
