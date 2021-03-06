package org.contakt.data.lineage

import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, Await, Future, ExecutionContext}
import scala.util.Try

/**
 * Class which provides utility methods for result maps.
 */
class ResultMap(implicit executionContext: ExecutionContext) {

  /** Thread-safe map for storing the result values. */
  private var results = new HashMap[String, Future[_]]()

  /**
   * Adds a result to the result map in a thread-safe way.
   * @param name name of the result.
   * @param value a Future value for the result.
   * @return whether the result was added or not.
   */
  def addResult[T](name: String, value: Future[T]): Boolean = {
    synchronized {
      if (results.isDefinedAt(name)) {
        // println(s"duplicated result name '$name', old = ${results(name)} (${results(name).value}), new = $value (${value.value})")
        val failurePromise = Promise[T]
        failurePromise failure (new DuplicatedResultNameException(name, results(name), value))
        results.put(name, failurePromise.future)
        false
      } else {
        results.put(name, value)
        true
      }
    }
  }

  /**
   * Returns a new result map containing the results from this result map and the given result map.
   * If the same named result occurs in both result maps, a future failure value is created in the combined map.
   * @param results result map to add to this map.
   * @return combined result map, possibly with some future failure values where a result was multiply defined.
   */
  def ++(results: ResultMap): ResultMap = {
    val combinedResults = new ResultMap()
    for (key <- keySet) { combinedResults.addResult(key, getResult(key)) }
    for (key <- results.keySet) { combinedResults.addResult(key, results.getResult(key)) }
    combinedResults
  }

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
  def getResult(name: String): Future[_] = results(name)

  /**
   * Tries to return a value for the given result name.
   * @param name name of the result.
   * @return a Success value containing a Future if there is a result for the name, or a Failure value otherwise.
   */
  def tryGetResult(name: String): Try[Future[_]] = Try{ getResult(name) }

  /**
   * Returns the results as an immutable map.
   * @return immutable name of the result names and matching future values.
   */
  def getResults: Map[String, Future[_]] = Map[String, Future[_]]() ++ results // convert to immutable map

  /**
   * Like 'getResults', but waits until all of the futures have completed, and returns their completion values.
   * Note: Ideally, process blocks should retain their inputs/outputs as futures until the last possible moment, to maximise opportunities for multi-threading.
   * @return immutable map of the result names and matching values.
   */
  def awaitResults: Map[String, Try[_]] = awaitResults(Duration.Inf)

  /**
   * Like 'awaitResults', but times out on any futures that don't complete within the duration 'atMost'.
   * Note: Ideally, process blocks should retain their inputs/outputs as futures until the last possible moment, to maximise opportunities for multi-threading.
   * @param atMost maximum duration after which futures are timed-out, yielding an exception as the completion value.
   * @return immutable map of the result names and matching values.
   */
  def awaitResults(atMost: Duration): Map[String, Try[_]] = {
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
class DuplicatedResultNameException(val name: String, val oldValue: Future[_], val newValue: Future[_]) extends Exception(s"duplicated string name in result map: $name: old value = $oldValue (${oldValue.value}), new value = $newValue (${newValue.value})") {}

/**
 * Exception for result validation errors.
 */
class ResultValidationException(label: Option[String], message: String, value: Any, cause: Option[Throwable]) extends ValidationException(label, message, value, cause) {}
