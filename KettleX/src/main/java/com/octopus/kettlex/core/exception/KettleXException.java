package com.octopus.kettlex.core.exception;

public class KettleXException extends RuntimeException {

  private static final String CR = System.getProperty("line.separator");
  /** Constructs a new throwable with null as its detail message. */
  public KettleXException() {
    super();
  }

  /**
   * Constructs a new throwable with the specified detail message.
   *
   * @param message - the detail message. The detail message is saved for later retrieval by the
   *     getMessage() method.
   */
  public KettleXException(String message) {
    super(message);
  }

  /**
   * Constructs a new throwable with the specified cause and a detail message of (cause==null ? null
   * : cause.toString()) (which typically contains the class and detail message of cause).
   *
   * @param cause the cause (which is saved for later retrieval by the getCause() method). (A null
   *     value is permitted, and indicates that the cause is nonexistent or unknown.)
   */
  public KettleXException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new throwable with the specified detail message and cause.
   *
   * @param message the detail message (which is saved for later retrieval by the getMessage()
   *     method).
   * @param cause the cause (which is saved for later retrieval by the getCause() method). (A null
   *     value is permitted, and indicates that the cause is nonexistent or unknown.)
   */
  public KettleXException(String message, Throwable cause) {
    super(message, cause);
  }

  /** get the messages back to it's origin cause. */
  @Override
  public String getMessage() {
    StringBuilder retval = new StringBuilder(CR);
    retval.append(super.getMessage()).append(CR);

    Throwable cause = getCause();
    if (cause != null) {
      String message = cause.getMessage();
      if (message != null) {
        retval.append(message).append(CR);
      } else {
        // Add with stack trace elements of cause...
        StackTraceElement[] ste = cause.getStackTrace();
        for (int i = ste.length - 1; i >= 0; i--) {
          retval
              .append(" at ")
              .append(ste[i].getClassName())
              .append(".")
              .append(ste[i].getMethodName())
              .append(" (")
              .append(ste[i].getFileName())
              .append(":")
              .append(ste[i].getLineNumber())
              .append(")")
              .append(CR);
        }
      }
    }

    return retval.toString();
  }

  public String getSuperMessage() {
    return super.getMessage();
  }
}
