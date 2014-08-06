package org.contakt.data

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions

/**
 * Utility object for data lineage classes.
 */
package object lineage {

  /**
   * Implicit conversion of a map with parameters to a 'ParameterMap' object with helper methods.
   * @param parameters map of named parameters
   * @return ParameterMap object for the parameters
   */
  implicit def mapToParameterMap(parameters: Map[Symbol, Any]): ParameterMap = new ParameterMap(parameters)

}
