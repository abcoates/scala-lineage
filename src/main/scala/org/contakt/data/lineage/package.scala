package org.contakt.data

import org.contakt.reactive.future.RichFuture

import scala.collection.immutable.Map
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions
import scala.language.postfixOps
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

  /**
   * Converts a Tuple2[Symbol,Any] into a Tuple2[String, Any].  Provided so that symbols can be used in name mappings for process blocks, parameters, results, etc.
   * @param tuple tuple for which the first item should be converted to a string.
   * @return a converted tuple with a string instead of a symbol
   */
  implicit def symbolTuple2ToStringTuple2[T](tuple: Tuple2[Symbol, T]): Tuple2[String, T] = (symbolToString(tuple._1), tuple._2)

  /**
   * Converts a value into a Future[Any], if it is not already a future.
   * @param value value to convert into a future, if necessary.
   * @return a future for the value.
   */
  implicit def valueToFuture[T](value: T): Future[_] = value match {
    case future: Future[_] => future
    case value => Future{ value }
  }

  /**
   * Converts a map from String -> Any to a map from String -> Future[Any].
   * @param valueMap a map from String -> Any.
   * @return a map from String -> Future[Any].
   */
  implicit def valueMapToFutureMap(valueMap: Map[String, _]): Map[String, Future[_]] = valueMap map { mapping =>
    mapping._1 -> valueToFuture(mapping._2)
  }

  /**
   * Converts a Future to a RichFuture.
   * @param future a Future.
   * @tparam T result type for the Future.
   * @return a RichFuture that adds new methods to the Future.
   */
  implicit def futureToRichFuture[T](future: Future[T]): RichFuture[T] = new RichFuture(future)

  /** Quick test that a 'Try' is a 'Success'. */
  def isSuccess(value: Try[_]): Boolean = value match {
    case Success(x) => true
    case _ => false
  }

  /** Quick test that a 'Try' is a 'Failure'. */
  def isFailure(value: Try[_]): Boolean = value match {
    case Failure(t) => true
    case _ => false
  }

  /**
   * Checks whether the given exception or throwable and its 'cause' chain match the given list of Throwable classes.
   * @param t exception or throwable to check.
   * @param classChain list of expected classes, starting at this throwable and following "cause" values.
   * @return whether the 'cause' chain matches the list of classes, or not.
   */
  def checkExceptionChain(t: Throwable, classChain: List[Class[_]]): Boolean = {
    classChain match {
      case Nil => t == null // a null exception matches an empty chain
      case head::tail =>
        if (Try{ head cast t} isSuccess) {
          checkExceptionChain(t getCause, tail.asInstanceOf[List[Class[_]]]) // if the head matches, try the rest of the tail
        } else {
          false
        }
      case _ => false
    }
  }

  /**
   * Extends "checkExceptionChain(Throwable, List[Class[_]])" to work with objects that might reasonably wrap a Throwable.
   * @param obj exception or throwable or wrapping object to check.
   * @param classChain list of expected classes, starting at the first throwable and following "cause" values.
   * @return whether the 'cause' chain matches the list of classes, or not.
   */
  def checkExceptionChain(obj: Any, classChain: List[Class[_]]): Boolean = {
    classChain match {
      case Nil =>
        obj match {
          case null => true
          case None => true
          case Success(x) => true
          case future: Future[_] => checkExceptionChain(future.value, classChain)
          case _ => false
        }
      case chain =>
        obj match {
          case Some(x) => checkExceptionChain(x, chain)
          case Failure(t) => checkExceptionChain(t, chain)
          case future: Future[_] => checkExceptionChain(future.value, classChain)
          case _ => false
        }
    }
  }

}
