package org.contakt.data.lineage

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.Map
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Await, Future, blocking}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration

/**
 * Class which provides utility methods for result maps.
 */
class ResultMap(implicit executor: ExecutionContext) {

  /** Thread-safe map used to collect results. */
  private var results = new ConcurrentHashMap[Symbol, Any]()

  /**
   * Puts a result in the result map.
   * If the result is a zero-parameter function, evaluates the function in a Future of a Try.
   * If the result is not a function, it should be an immutable value.
   * If there is already a value for the name, a list of values is created as the result for the name.
   * @param name name of the result
   * @param value value for the result
   * @return the result value.  If 'value' was a zero-parameter function, returns a Future(Try(...)) value for the function evaluation.
   */
  def putResult(name: Symbol, value: Any): Any = {
    value match {
      case value: Function0[Any] =>
        val valueFuture = Future{ Try{ (value.asInstanceOf[Function0[Any]])() } }
        checkPutResult(name, valueFuture)
        valueFuture
      case value =>
        checkPutResult(name, value)
        value
    }
  }

  /** Thread-safe method to put a result to the results map.  Creates a results list for the name if there is already a value. */
  private def checkPutResult(name: Symbol, value: Any) {
    blocking {
      synchronized {
        if (results.containsKey(name)) {
          results.put(name, value :: results.get(name) :: Nil)
        } else {
          results.put(name, value)
        }
      }
    }
  }

  /**
   * Retrieves the given named result, if possible.
   * @param name name of the result
   * @return The value for the result in a Success, or an exception in a Failure.
   */
  def tryResult(name: Symbol) = ???

  /**
   * Like 'tryResult', but if the result value is a Future, waits for it to complete with no timeout.
   * Note that ideally, process blocks should be written to work with futures directly as much as possible.
   * @param name name of the result.
   * @return The value for the result in a Success, or an exception in a Failure.
   */
  def awaitResult(name: Symbol): Try[Any] = awaitResult(name, Duration.Inf)

  /**
   * Like 'tryResult', but if the result value is a Future, waits for it to complete with the given duration.
   * Note that ideally, process blocks should be written to work with futures directly as much as possible.
   * @param name name of the result.
   * @param atMost timeout duration for waiting for completion of a 'Future' value.
   * @return The value for the result in a Success, or an exception in a Failure.
   */
  def awaitResult(name: Symbol, atMost: Duration): Try[Any] = ???

  /**
   * Retrieves the given named result, if possible, unwrapping the value if it is a 'Success' value.
   * @param name name of the result.
   * @return The Option containing the result value, if one exists, otherwise None.
   */
  def getResult(name: Symbol): Option[Any] = ???

  /**
   * Retrieves the given named result, if possible, otherwise returns the provided default value.
   * @param name name of the result
   * @param default default value for the result
   * @return A value for the result, either the retrieved value or the default value.
   */
  def getResultOrElse(name: Symbol, default: => Any): Any = ???

  /**
   * Returns the contents of the ResultMap as an immutable Scala map.
   * @return map of reults.  Some of the results may be Future(Try(...)) values.
   */
  def getResultMap: Map[Symbol, Any] = ???

  /**
   * Like 'getResultMap', but if the result value is a Future, waits for it to complete with no timeout.
   * Note that ideally, process blocks should be written to work with futures directly as much as possible.
   * @return map of results, with any futures resolved to Try values.
   */
  def awaitResultMap: Map[Symbol, Any] = awaitResultMap(Duration.Inf)

  /**
   * Like 'getResultMap', but if the result value is a Future, waits for it to complete with the given timeout.
   * Note that ideally, process blocks should be written to work with futures directly as much as possible.
   * @return map of results, with any futures resolved to Try values.
   */
  def awaitResultMap(atMost: Duration): Map[Symbol, Any] = ???

  /**
   * Returns the number of results.
   * @return the number of results.
   */
  def size = results.size

}
