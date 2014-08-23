package org.contakt.data.lineage

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.{Success, Failure, Try}

/**
* Trait for a function that can be used to validate a future, returning the same future is successful, or a Future(Failure(...)) if validation fails.
*/
abstract class Validation[T] extends Function1[Future[T], Future[T]] {
//
//  /**
//   * Combines two validations via an 'and' operation.
//   * @param vld validation to 'and' with this validation.
//   * @return new 'and' validation.
//   */
//  def &&(vld: Validation): Validation = new Validation {
//
//    def apply[T](future: Future[T]): Future[T] = {
//      Await.ready(future, Duration.Inf)
//      future.value.get match {
//        case failure: Failure[_] => future
//        case _ =>
//          val validatedFuture = this(future)
//          Await.ready(validatedFuture, Duration.Inf)
//          validatedFuture.value.get match {
//            case failure: Failure[_] => validatedFuture
//            case _ => vld(future) // if the 1st validation passed, run the 2nd validation
//          }
//      }
//    }
//
//  }
//
//  /**
//   * Combines two validations via an 'or' operation.
//   * @param vld validation to 'or' with this validation.
//   * @return new 'or' validation.
//   */
//  def ||(vld: Validation): Validation = new Validation {
//
//    def apply[T](future: Future[T]): Future[T] = {
//      Await.ready(future, Duration.Inf)
//      future.value.get match {
//        case failure: Failure[_] => future
//        case _ =>
//          val validatedFuture = this(future)
//          Await.ready(validatedFuture, Duration.Inf)
//          validatedFuture.value.get match {
//            case success: Success[_] => future // if the 1st validation passed, skip the second validation
//            case _ => vld(future)
//          }
//      }
//    }
//
//  }

}

/**
* Companion object for Validation trait.
*/
object Validation {

  /** A pass-through validation that doesn't check anything. */
  def NONE = new Validation[Any] {
    override def apply(future: Future[Any]): Future[Any] = future
  }

  def hasResultClass(clazz: Class[_])(implicit executionContext: ExecutionContext) = new Validation[Any] {
    override def apply(future: Future[Any]): Future[Any] = future map { value => // note use of 'map' to create a new Future based on the eventual completed value of 'future', with exceptions passed through automatically
      try {
        clazz cast value // check if value is a valid 'clazz'
      }
      catch {
        case _: Throwable => throw new ValidationException(s"value doesn't match class '${clazz.getCanonicalName}': $value", value)
      }
    }
  }

  def equalTo(expectedValue: Any)(implicit executionContext: ExecutionContext) = new Validation[Any] {
    override def apply(future: Future[Any]): Future[Any] = future map { value => // note use of 'map' to create a new Future based on the eventual completed value of 'future', with exceptions passed through automatically
      if (value == expectedValue) {
        value
      } else {
        throw new ValidationException(s"value doesn't equal expected value '$expectedValue': $value", value)
      }
    }
  }

}

/**
* Exception for validation errors.
*/
class ValidationException(message: String, value: Any) extends Exception(message) {}
