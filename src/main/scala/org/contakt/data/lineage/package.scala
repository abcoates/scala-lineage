package org.contakt.data

import scala.collection.generic.FilterMonadic
import scala.collection.immutable.Map
import scala.concurrent.{Promise, Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
 * Utility object for data lineage classes.
 */
package object lineage {

  /**
   * Converts a symbol into a string.  Provided so that symbols can be used as names for process blocks, parameters, results, etc.
   * @param symbol symbol to convert to a string.
   * @return string equivalent of the symbol (with no leading apostrophe).
   */
  implicit def symbolToString(symbol: Symbol): String = symbol.name

//  /**
//   * Converts a Tuple2[Symbol,Any] into a Tuple2[String, Any].  Provided so that symbols can be used in name mappings for process blocks, parameters, results, etc.
//   * @param tuple tuple for which the first item should be converted to a string.
//   * @return a converted tuple with a string instead of a symbol
//   */
//  implicit def symbolTuple2ToStringTuple2[T](tuple: Tuple2[Symbol, T]): Tuple2[String, T] = (symbolToString(tuple._1), tuple._2)
//
//  /**
//   * Converts a value into a Future[Any], if it is not already a future.
//   * @param value value to convert into a future, if necessary.
//   * @return a future for the value.
//   */
//  implicit def valueToFuture(value: Any): Future[Any] = value match {
//    case future: Future[Any] => future
//    case value => Future{ value }
//  }
//
// /**
//   * Converts a map from String -> Any to a map from String -> Try[Any].
//   * @param map a map from String -> Any.
//   * @return a map from String -> Try[Any].
//   */
//  implicit def valueMapToTryMap(map: Map[String, Any]): Map[String, Future[Any]] = map.map{mapping =>
//    mapping._1 -> valueToFuture(mapping._2)
//  }

  /**
   * Converts a future into a typed future.
   * @param f a plain future.
   * @tparam T type of the result of the future.
   * @return a typed future containing the future and its return type.
   */
  implicit def futureToTypedFuture[T](f: Future[T])(implicit manifest: Manifest[T]): TypedFuture[T] = TypedFuture[T](f)


  /**
   * Converts a parameterless function into a typed future.
   * @param f a parameterless function.
   * @tparam T type of the result of the function.
   * @return a typed future containing the future and its return type.
   */
  implicit def functionToTypedFuture[T](f: () => T)(implicit manifest: Manifest[T]): TypedFuture[T] = TypedFuture[T](Future{f()})

}
