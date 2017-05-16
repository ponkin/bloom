package com.github.ponkin.bloom;

/**
 * Special Exception, thrown when
 * two incompatible filters are merged.
 *
 * @author Alexey Ponkin
 */
public class IncompatibleMergeException extends Exception {

  /**
   * Constructor as usual with
   * String message that will be displayed
   * in stacktrace
   *
   * @param message string message to print in stacktrace
   */
  public IncompatibleMergeException(String message){
    super(message);
  }
}
