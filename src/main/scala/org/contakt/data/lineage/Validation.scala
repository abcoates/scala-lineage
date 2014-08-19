package org.contakt.data.lineage

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Success, Failure, Try}

/**
 * Trait for a function that can be used to validate a future, returning the same future is successful, or a Future(Failure(...)) if validation fails.
 */
trait Validation extends Function1[Future[Any], Future[Any]] {

  /**
   * Combines two validations via an 'and' operation.
   * @param vld validation to 'and' with this validation.
   * @return new 'and' validation.
   */
  def &&(vld: Validation): Validation = new Validation {

    def apply(future: Future[Any]): Future[Any] = {
      Await.ready(future, Duration.Inf)
      future.value.get match {
        case failure: Failure[_] => future
        case _ =>
          val validatedFuture = this(future)
          Await.ready(validatedFuture, Duration.Inf)
          validatedFuture.value.get match {
            case failure: Failure[_] => validatedFuture
            case _ => vld(future) // if the 1st validation passed, run the 2nd validation
          }
      }
    }

  }

  /**
   * Combines two validations via an 'or' operation.
   * @param vld validation to 'or' with this validation.
   * @return new 'or' validation.
   */
  def ||(vld: Validation): Validation = new Validation {

    def apply(future: Future[Any]): Future[Any] = {
      Await.ready(future, Duration.Inf)
      future.value.get match {
        case failure: Failure[_] => future
        case _ =>
          val validatedFuture = this(future)
          Await.ready(validatedFuture, Duration.Inf)
          validatedFuture.value.get match {
            case success: Success[_] => future // if the 1st validation passed, skip the second validation
            case _ => vld(future)
          }
      }
    }

  }

}

/**
 * Companion object for Validation trait.
 */
object Validation {

  /** A pass-through validation that doesn't check anything. */
  val NONE: Validation = new Validation {
    def apply(future: Future[Any]) = future
  }

//  def sameClassAs(sampleValue: Any): Validation = new Validation {
//    // TODO: update & fix - current implementation is for Try values, not Future values
//    def apply(future: Future[Any]): Future[Any] = future match {
//      case failure @ Failure(t) => failure
//      case success @ Success(x) => if (x.getClass == sampleValue.getClass) success else Failure(new ClassCastException(s"value '$x' could not be cast to class '${sampleValue.getClass.getName}'"))
//    }
//  }
//
//  def equalTo(expectedValue: Any): Validation = new Validation {
//    // TODO: update & fix - current implementation is for Try values, not Future values
//    def apply(future: Future[Any]): Future[Any] = future match {
//      case failure @ Failure(t) => failure
//      case success @ Success(x) => if (x == expectedValue) success else Failure(new ValidationException(s"value '$x' not equal to expected value '$expectedValue'", x))
//    }
//  }

}

/**
 * Exception for validation errors.
 */
class ValidationException(message: String, value: Any) extends Exception(message) {}