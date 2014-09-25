package org.contakt.data.lineage

import scala.collection.immutable.Map
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Class which provides utility methods for parameter maps.
 */
class ParameterMap(parameters: Map[String, Future[Any]])(implicit executionContext: ExecutionContext) {

  /**
   * Retrieves the given named parameter, if possible.
   * @param name name of the parameter
   * @return A future for the value for the parameter.
   */
  def apply(name: String) = getParameter(name)

  /**
   * Retrieves the given named parameter, if possible.
   * @param name name of the parameter
   * @return A future for the value for the parameter.
   * @throws NoSuchElementException if there is no parameter value matching the name.
   */
  def getParameter(name: String): Future[Any] = parameters(name)

  /**
   * Tries to return a value for the given parameter name.
   * @param name name of the parameter.
   * @return a Success value containing a Future if there is a parameter for the name, or a Failure value otherwise.
   */
  def tryParameter(name: String): Try[Future[Any]] = Try{ getParameter(name) }

  /**
   * Adds the given parameter map to this map.  If the same named parameter occurs in both parameter maps, a Future(Failure(...)) value is created in the combined map.
   * @param parameters parameter map to add to this map.
   * @return combined parameter map, possibly with some Future(Failure(...)) values where a parameter was multiply defined.
   */
  def ++(parameters: ParameterMap): ParameterMap = {
    val combinedMap = new HashMap[String, Future[Any]]()
    for (key <- keySet) {
      if (parameters isDefinedAt key) { // check for erroneous multiply defined parameter values
        combinedMap(key) = Future{ throw new DuplicatedParameterNameException(key, this(key), parameters(key)) }
      } else {
        combinedMap(key) = this(key)
      }
    }
    for (key <- parameters.keySet if !isDefinedAt(key)) {
      combinedMap(key) = parameters(key)
    }
    new ParameterMap(Map[String, Future[Any]]() ++ combinedMap)
  }

  /**
   * Whether the given parameter name has a value defined for it, or not.
   * @param name name of the parameter.
   * @return whether there is a parameter for the given name.
   */
  def isDefinedAt(name: String) = parameters isDefinedAt name

  /**
   * Returns the number of parameters.
   * @return the number of parameters.
   */
  def size = parameters.size

  /**
   * Returns a set of the defined parameter names.
   * @return a set of parameter name strings.
   */
  def keySet = parameters.keySet

}

/**
 * Exception for duplicated parameter errors.
 */
class DuplicatedParameterNameException(name: String, oldValue: Future[_], newValue: Future[_]) extends Exception(s"duplicated string name in parameter map: $name: old value = ($oldValue), new value = ($newValue)") {}

/**
 * Exception for parameter validation errors.
 */
class ParameterValidationException(label: Option[String], message: String, value: Any, cause: Option[Throwable]) extends ValidationException(label, message, value, cause) {}
