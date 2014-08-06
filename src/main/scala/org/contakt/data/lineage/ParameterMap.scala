package org.contakt.data.lineage

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration.Duration

/**
 * Class which provides utility methods for parameter maps.
 */
class ParameterMap(parameters: Map[Symbol, Any])(implicit executor: ExecutionContext) {

  /**
   * Retrieves the given named parameter, if possible.
   * @param name name of the parameter
   * @return The value for the parameter in a Success , or an exception in a Failure.
   */
  def tryParameter(name: Symbol): Try[Any] = {
    // TODO: if the value is a collection of values, apply the logic below to all of those values
    if (parameters.contains(name)) {
      parameters(name) match {
        case success: Success[_] => success.get match { // if the value is a Success, check if the Success wraps a completed future
          case future: Future[Any] if future.isCompleted => future.value.get // if the future is completed, unpack the 'Try' result.
          case value => success
        }
        case tryValue: Try[Any] => tryValue // pass through any other Try values unchanged
        case future: Future[Any] if future.isCompleted => future.value.get // if the future is completed, unpack the 'Try' result.
        case future: Future[Any] => Success(Future) // pass through uncompleted futures as a success.
        case ex: Throwable => Failure(ex) // treat any throwable/exception values as a failure
        case null => Failure(new NullPointerException("null parameter value for name: " + name.toString)) // treat null values as a failure condition
        case value => Success(value) // pass through any other value as a success.
      }
    } else {
      Failure(new NoSuchElementException("no parameter for name: " + name.toString)) // treat non-existance of the parameter name in the parameter map as a failure condition
    }
  }

  /**
   * Like 'tryParameter', but if the parameter value is a Future, waits for it to complete with no timeout.
   * Note that ideally, process blocks should be written to work with futures directly as much as possible.
   * @param name name of the parameter.
   * @return The value for the parameter in a Success, or an exception in a Failure.
   */
  def awaitParameter(name: Symbol): Try[Any] = awaitParameter(name, Duration.Inf)

  /**
   * Like 'tryParameter', but if the parameter value is a Future, waits for it to complete with the given duration.
   * Note that ideally, process blocks should be written to work with futures directly as much as possible.
   * @param name name of the parameter.
   * @param atMost timeout duration for waiting for completion of a 'Future' value.
   * @return The value for the parameter in a Success, or an exception in a Failure.
   */
  def awaitParameter(name: Symbol, atMost: Duration): Try[Any] = ???

  /**
   * Retrieves the given named parameter, if possible, unwrapping the value if it is a 'Success' value.
   * @param name name of the parameter.
   * @return The Option containing the parameter value, if one exists, otherwise None.
   */
  def getParameter(name: Symbol): Option[Any] = ???

  /**
   * Retrieves the given named parameter, if possible, otherwise returns the provided default value.
   * @param name name of the parameter
   * @param default default value for the parameter
   * @return A value for the parameter, either the retrieved value or the default value.
   */
  def getParameterOrElse(name: Symbol, default: => Any): Any = ???

  /**
   * Returns the number of parameters.
   * @return the number of parameters.
   */
  def size = parameters.size

}
