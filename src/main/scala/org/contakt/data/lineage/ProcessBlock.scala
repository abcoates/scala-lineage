package org.contakt.data.lineage

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

/**
 * Trait for processing blocks that take named inputs and produce named outputs.
 */
trait ProcessBlock {

  // **** Methods which need to be defined.

  /**
   * Name which uniquely identifies the process block.
   * @return the name of the process block.
   */
  def name: String

  /**
   * Defines the parameters for a process block, and validations for those parameters.
   * Note: must include all parameters, including constant parameters defined by 'constParameters'.
   * @return map of name strings and matching validations.
   */
  def parameters: Map[String, Validation]

  /**
   * Defines the results for a process block, and validations for those results.
   * @return map of name strings and matching validations.
   */
  def results: Map[String, Validation]

  /**
   * Core processing function for the process block.
   * Note: the core processing function should <b>not</b> be called directly by users.  It is called by 'run'.
   * @return function which is run by 'run' to perform the core processing.
   */
  def process: ParameterMap => ResultMap

  // **** Methods which have a default implementation.

  /** Shorthand for 'run'. */
  def apply(parameters: ParameterMap) = run(parameters)

  /**
   * Constant parameters that are always automatically passed to 'run'.
   * Note: when constant parameters are used, the same parameters must <b>not</b> be passed via the 'parameters' for 'run'.
   * @return a parameter map with the constant parameters.
   */
  def constParameters = new ParameterMap(Map[String, Future[Any]]())(executionContext) // empty by default

  /** Excution context for futures. */
  implicit def executionContext = scala.concurrent.ExecutionContext.Implicits.global

  /**
   * Processes the given map of parameters and returns a map of results (some of which may be futures).
   * @param parameters Map of named parameters.  All values should be immutable to allow multi-threaded processing.  If the value is a 'Failure' value, if is <b>*not*</b> passed to the process block.
   * @return A map of named results.
   */
  def run(parameters: ParameterMap)(implicit executor: ExecutionContext = executionContext): ResultMap = {
    val actualParameters = validateParameters(parameters ++ constParameters)
    val results = process(actualParameters)
    val actualResults = validateResults(results)
    actualResults
  }

  /**
   * Validates the given parameters and produces a new parameter map where non-validating parameters are converted to Future(Failure(...)) values.
   * @param runParameters parameters to validate.
   * @return validated parameters, some of which may have been converted to futures of failures.
   */
  private def validateParameters(runParameters: ParameterMap): ParameterMap = {
    runParameters // TODO: implement this
  }

  /**
   * Validates the given results and produces a new result map where non-validating results are converted to Future(Failure(...)) values.
   * @param runResults results to validate.
   * @return validated results, some of which may have been converted to futures of failures.
   */
  private def validateResults(runResults: ResultMap): ResultMap = {
    runResults // TODO: implement this
  }

  // TODO: add a method to return a dynamically-generated process map.

}
