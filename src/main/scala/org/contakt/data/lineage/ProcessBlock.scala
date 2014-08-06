package org.contakt.data.lineage

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Trait for processing blocks that take named inputs and produce named outputs.
 */
trait ProcessBlock {

  // **** Methods which need to be defined.

  /**
   * Processes the given map of parameters and returns a map of results (some of which may be futures).
   * @param parameters Map of named parameters.  All values should be immutable to allow multithreaded processing.  If the value is a 'Failure' value, if is <b>*not*</b> passed to the process block.
   * @return A map of named results.
   */
  def run(parameters: ParameterMap)(implicit executor: ExecutionContext): ResultMap

  // **** Methods which have a default implementation.

  /** Shorthand for 'run'. */
  def apply(parameters: ParameterMap) = run(parameters)

  /** Excution context for futures. */
  implicit def executionContext = global

}
