package org.contakt.data.lineage

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Defines a mapping from a result map into a new parameter map.
 */
class ResultToParameterMapping(val nameMap: Map[String, String], val defaultMapping: ResultToParameterDefault = DEFAULT_MAP_NONE)(implicit executionContext: ExecutionContext) {

  /**
   * Applies the mapping to the given results to produce a set of parameters.
   * @param results result set for mapping
   * @return mapped parameters from result set
   */
  def map(results: ResultMap): ParameterMap = {
    val parameters = new mutable.HashMap[String, Future[Any]]() // temporary map for parameters

    for (key <- results.keySet) {
      if (nameMap.contains(key)) {
        parameters put (nameMap(key), results(key))
      } else {
        defaultMapping match {
          case DEFAULT_MAP_ALL => parameters put (key, results(key))
          case DEFAULT_MAP_NONE => // do mothing
        }
      }
    }

    new ParameterMap(Map[String, Future[Any]]() ++ parameters)(executionContext) // return result parameter map
  }

  /**
   * Postfix operator which provides a shorthand for 'map', e.g. 'resultsMap :: resultToParameterMapping'
   * @param results result set for mapping
   * @return mapped parameters from result set
   */
  def ::(results: ResultMap): ParameterMap = map(results)

  /**
   * Adds a mapping from a result name to a parameter name.
   * @param mapping result (from) and parameter (to) name pair
   * @return new ResultToParameterMapping with the additional mapping
   */
  def addMapping(mapping: (String, String)): ResultToParameterMapping = {
    new ResultToParameterMapping(nameMap + mapping, defaultMapping)
  }

  /**
   * Postfix operator which provides a shorthand for 'addMapping', e.g. '("a","b") :: initialMapping'
   * @param mapping result (from) and parameter (to) name pair
   * @return new ResultToParameterMapping with the additional mapping
   */
  def ::(mapping: (String, String)): ResultToParameterMapping = addMapping(mapping)

  /**
   * Adds a mapping from a result name to an identical parameter name.
   * @param name name of both the result and the matching parameter, i.e. a same-name mapping
   * @return new ResultToParameterMapping with the additional mapping
   */
  def addResult(name: String): ResultToParameterMapping = addMapping((name, name))

  /**
   * Postfix operator which provides a shorthand for 'addResult', e.g. ' "a" :: initialMapping'
   * @param name name of both the result and the matching parameter, i.e. a same-name mapping
   * @return new ResultToParameterMapping with the additional mapping
   */
  def ::(name: String): ResultToParameterMapping = addResult(name)

  /**
   * Adds mappings from result names to a parameter names.
   * @param mappings result to parameter name mappings
   * @return new ResultToParameterMapping with the additional mappings
   */
  def addMappings(mappings: Map[String, String]): ResultToParameterMapping = {
    new ResultToParameterMapping(nameMap ++ mappings, defaultMapping)
  }

  /**
   * Postfix operator which provides a shorthand for 'addMappings', e.g. 'Map("a"->"b", "c"->"d") :: initialMapping'
   * @param mappings result to parameter name mappings
   * @return new ResultToParameterMapping with the additional mappings
   */
  def ::(mappings: Map[String, String]): ResultToParameterMapping = addMappings(mappings)

  /**
   * Adds mappings from result names to identical parameter names.
   * @param names names of both results and the matching parameters, i.e. all same-name mappings
   * @return new ResultToParameterMapping with the additional mappings
   */
  def addResults(names: List[String]): ResultToParameterMapping = addMappings((names map {name => (name,name)}).toMap[String,String])

  /**
   * Postfix operator which provides a shorthand for 'addResults', e.g. 'List("a","b") :: initialMapping'
   * @param names names of both results and the matching parameters, i.e. all same-name mappings
   * @return new ResultToParameterMapping with the additional mappings
   */
  def ::(names: List[String]): ResultToParameterMapping = addResults(names)

}

/**
 * Enumerated case class used to define the default behaviour of a result-to-parameter mapping.
 * @param name name of the enumerated value.
 */
sealed case class ResultToParameterDefault(name: Symbol) {}

/** Default to mapping all results to a parameter of the same name, unless there is an explicit mapping. */
object DEFAULT_MAP_ALL extends ResultToParameterDefault('ALL)

/** Default to no mapping of results to parameters of the same name, unless there is an explicit mapping. */
object DEFAULT_MAP_NONE extends ResultToParameterDefault('NONE)
