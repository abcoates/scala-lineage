package org.contakt.data.lineage

import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.util.Try

/**
 * Class which provides utility methods for result maps.
 */
class ResultMap(implicit executionContext: ExecutionContext) {

  /** Thread-safe map for storing the result values. */
  private var results = new HashMap[String, Future[Any]]()

  /**
   * Adds a result to the result map in a thread-safe way.
   * @param name name of the result.
   * @param value a Future value for the result.
   * @throws DuplicatedResultNameException if the map already contains a result with the same name.
   *
   */
  def addResult(name: String, value: Future[Any]) {
    synchronized {
      if (results.isDefinedAt(name)) {
        throw new DuplicatedResultNameException(name, results.get(name), value)
      }
      results.put(name, value)
    }
  }

  /**
   * Tries to add a result to the result map in a thread-safe way.
   * @param name name of the result.
   * @param value a Future value for the result.
   * @return a Success value if the value could be added, or a Failure value otherwise.
   */
  def tryAddResult(name: String, value: Future[Any]) = Try{ addResult(name, value) }

  /**
   * Whether the given result name has a value defined for it, or not.
   * @param name name of the result.
   * @return whether there is a result for the given name.
   */
  def isDefinedAt(name: String) = results isDefinedAt name

  /**
   * Returns the result for the given name.
   * @param name name of the result to return.
   * @return result value for the name.(
   * @throws NoSuchElementException if there is no result matching the given name.
   */
  def apply(name: String) = getResult(name)

  /**
   * Returns the result for the given name.
   * @param name name of the result to return.
   * @return result value for the name.(
   * @throws NoSuchElementException if there is no result matching the given name.
   */
  def getResult(name: String): Future[Any] = results(name)

  /**
   * Tries to return a value for the given result name.
   * @param name name of the result.
   * @return a Success value containing a Future if there is a result for the name, or a Failure value otherwise.
   */
  def tryResult(name: String): Try[Future[Any]] = Try{ getResult(name) }

  /**
   * Returns the results as an immutable map.
   * @return immutable name of the result names and matching future values.
   */
  def getResults: Map[String, Future[Any]] = Map[String, Future[Any]]() ++ results // convert to immutable map

  /**
   * Like 'getResults', but waits until all of the futures have completed, and returns their completion values.
   * Note: Ideally, process blocks should retain their inputs/outputs as futures until the last possible moment, to maximise opportunities for multi-threading.
   * @return immutable map of the result names and matching values.
   */
  def awaitResults: Map[String, Try[Any]] = awaitResults(Duration.Inf)

  /**
   * Like 'awaitResults', but times out on any futures that don't complete within the duration 'atMost'.
   * Note: Ideally, process blocks should retain their inputs/outputs as futures until the last possible moment, to maximise opportunities for multi-threading.
   * @param atMost maximum duration after which futures are timed-out, yielding an exception as the completion value.
   * @return immutable map of the result names and matching values.
   */
  def awaitResults(atMost: Duration): Map[String, Try[Any]] = {
    getResults map { mapping => mapping._1 -> Try{ Await.result(mapping._2, atMost) } }
  }

  /**
   * Return the number of named results in the result set.
   * @return number of named results.
   */
  def size = results.size

  /**
   * Returns a set of the defined result names.
   * @return a set of result name strings.
   */
  def keySet = results.keySet

}

/**
 * Exception for duplicated result errors.
 */
class DuplicatedResultNameException(name: String, oldValue: Any, newValue: Any) extends Exception(s"duplicated string name in result map: $name: old value = ($oldValue), new value = ($newValue)") {}

/**
 * Exception for result validation errors.
 */
class ResultValidationException(label: Option[String], message: String, value: Any, thrown: Option[Throwable]) extends ValidationException(label, message, value, thrown) {}
