package org.contakt.data.lineage

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Try, Failure}

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
  def parameterChecks: Map[String, Validation[_]]

  /**
   * Defines the results for a process block, and validations for those results.
   * @return map of name strings and matching validations.
   */
  def resultChecks: Map[String, Validation[_]]

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
  def constParameters = new ParameterMap(Map[String, Future[_]]())(executionContext) // empty by default

  /** Execution context for futures.  Override to use a different context. */
  implicit def executionContext = scala.concurrent.ExecutionContext.Implicits.global

  /**
   * Processes the given map of parameters and returns a map of results (some of which may be futures).
   * @param parameters Map of named parameters.  All values should be immutable to allow multi-threaded processing.  If the value is a 'Failure' value, if is <b>*not*</b> passed to the process block.
   * @return A map of named results.
   */
  def run(parameters: ParameterMap)(implicit executor: ExecutionContext = executionContext): ResultMap = {
    val actualParameters = validateParameters(parameters ++ constParameters)
    validateResults(process(actualParameters))
  }

  /**
   * Validates the given parameters and produces a new parameter map where non-validating parameters are converted to Future(Failure(...)) values.
   * @param runParameters parameters to validate.
   * @return validated parameters, some of which may have been converted to futures of failures.
   */
  private def validateParameters(runParameters: ParameterMap): ParameterMap = {
    val validatedMapSet = for (param <- parameterChecks.keySet) yield {
      if (runParameters isDefinedAt param) {
        val validation = parameterChecks(param).asInstanceOf[Validation[Any]]
        param -> (validation(runParameters(param)) collectValue {
          case Failure(t) => Failure(new ParameterValidationException(
            Some(s"parameter '$param' in process block '${name}'"),
            s"parameter failed validation: $param",
            runParameters(param),
            Some(t)
          ))
          case other: Try[_] => other
        })
      } else {
        param -> Future {
          throw new ParameterValidationException(
            Some(s"missing parameter in '$param' in process block '${name}'"),
            s"no parameter found with name: $param",
            None,
            Some(new NoSuchElementException(s"missing parameter: $param"))
          )
        }
      }
    }
    val validatedMap = validatedMapSet.toMap[String, Future[_]]
    new ParameterMap(validatedMap)
  }

  /**
   * Validates the given results and produces a new result map where non-validating results are converted to Future(Failure(...)) values.
   * @param runResults results to validate.
   * @return validated results, some of which may have been converted to futures of failures.
   */
  private def validateResults(runResults: ResultMap): ResultMap = {
    val newResultMap = new ResultMap()
    for (result <- resultChecks.keySet) yield {
      if (runResults isDefinedAt result) {
        val validation = resultChecks(result).asInstanceOf[Validation[Any]]
        newResultMap.addResult(
          result,
          validation(runResults(result)) collectValue {
            case Failure(t) => Failure(new ResultValidationException(
              Some(s"result '$result' in process block '${name}'"),
              s"result failed validation: $result",
              runResults(result),
              Some(t)
            ))
            case other: Try[_] => other
          }
        )
      } else {
        newResultMap.addResult(result, Future{
          throw new ResultValidationException(
            Some(s"missing result in '$result' in process block '${name}'"),
            s"no result found with name: $result",
            None,
            Some(new NoSuchElementException(s"missing result: $result"))
          )
        })
      }
    }
    newResultMap
  }

  // TODO: add a method to return a dynamically-generated process map.

}

// TODO: create a class for mapping results into parameters
// TODO: it may map all or none by default to the same name
// TODO: it may rename a value, or it may exclude it
// TODO: however, there is no transformation/modification of the result values
// TODO: there should be a fluent/DSL way to have one process after another in the code,
// TODO: with a clear mapping from the results of the first to the parameters of the second