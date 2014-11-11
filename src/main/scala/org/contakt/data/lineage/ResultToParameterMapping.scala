package org.contakt.data.lineage

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Defines a mapping from a result map into a new parameter map.
 */
class ResultToParameterMapping(val nameMap: Map[String, String], val defaultMapping: ResultToParameterDefault = MAP_NONE)(implicit executionContext: ExecutionContext) {

  // TODO: Applies the name mapping to a set of results in order to create a new set of parameters.
  def map(results: ResultMap): ParameterMap = {
    val parameters = new mutable.HashMap[String, Future[Any]]() // temporary map for parameters

    for (key <- results.keySet) {
      if (nameMap.contains(key)) {
        parameters put (nameMap(key), results(key))
      } else {
        defaultMapping match {
          case MAP_ALL => parameters put (key, results(key))
          case MAP_NONE => // do mothing
        }
      }
    }

    new ParameterMap(Map[String, Future[Any]]() ++ parameters)(executionContext) // return result parameter map
  }

  // TODO: returns a result-to-parameter mapping with an extra name mapping.
  def addMapping(mapping: (String, String)): ResultToParameterMapping = {
    new ResultToParameterMapping(nameMap + mapping, defaultMapping)
  }

  // TODO: scaladoc
  def addMappings(mappings: Map[String, String]): ResultToParameterMapping = {
    new ResultToParameterMapping(nameMap ++ mappings, defaultMapping)
  }

  // TODO: scaladoc
  def ::(mapping: (String, String)): ResultToParameterMapping = addMapping(mapping)

  // TODO: scaladoc
  def ::(mappings: Map[String, String]): ResultToParameterMapping = addMappings(mappings)

  // TODO: scaladoc
  def ::(results: ResultMap): ParameterMap = map(results)

}

//class ResultToParameterMapping_MAP_ALL(implicit executionContext: ExecutionContext) extends ResultToParameterMapping(Map[String, String](), ALL)(executionContext)
//
//class ResultToParameterMapping_MAP_NONE(implicit executionContext: ExecutionContext) extends ResultToParameterMapping(Map[String, String](), NONE)(executionContext)

/**
 * Enumerated case class used to define the default behaviour of a result-to-parameter mapping.
 * @param name name of the enumerated value.
 */
sealed case class ResultToParameterDefault(name: Symbol) {}

/** Default to mapping all results to a parameter of the same name, unless there is an explicit mapping. */
object MAP_ALL extends ResultToParameterDefault('ALL)

/** Default to no mapping of results to parameters of the same name, unless there is an explicit mapping. */
object MAP_NONE extends ResultToParameterDefault('NONE)
