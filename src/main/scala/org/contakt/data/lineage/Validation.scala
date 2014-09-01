package org.contakt.data.lineage

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.{Success, Failure, Try}

/**
* Trait for a function that can be used to validate a future, returning the same future is successful, or a Future(Failure(...)) if validation fails.
*/
abstract class Validation[T](label: Option[String] = None) extends Function1[Future[T], Future[T]] {

//  /**
//   * Combines two validations via an 'and' operation.
//   * @param vld validation to 'and' with this validation.
//   * @return new 'and' validation.
//   */
//  def &&(vld: Validation) = new Validation[T] {
//
//    // TODO: update this to use something akin to 'map', but which provides a 'Try'
//    def apply(future: Future[T]): Future[T] = {
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
//  def ||(vld: Validation) = new Validation[T] {
//
//    // TODO: update this to use something akin to 'map', but which provides a 'Try'
//    def apply(future: Future[T]): Future[T] = {
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
    override def apply(future: Future[Any]) = future
  }

  def hasResultClass(clazz: Class[_])(implicit executionContext: ExecutionContext) = new Validation[Any] {
    override def apply(future: Future[Any]) = future map { value => // note use of 'map' to create a new Future based on the eventual completed value of 'future', with exceptions passed through automatically
      try {
        val castValue = clazz cast value // check if value is a valid 'clazz'
        // println(s"cast: $value [${value.getClass.getName}] => $castValue [${castValue.getClass.getName}]")
        castValue
      }
      catch {
        case t: Throwable => throw new ValidationException(Some(s"${classOf[Validation[Any]].getName}#hasResultClass"), s"value doesn't match class '${clazz.getName}': $value", value, Some(t))
      }
    }
  }

  def equalTo(expectedValue: Any)(implicit executionContext: ExecutionContext) = new Validation[Any] {
    override def apply(future: Future[Any]) = future map { value => // note use of 'map' to create a new Future based on the eventual completed value of 'future', with exceptions passed through automatically
      if (value == expectedValue) {
        value
      } else {
        throw new ValidationException(Some(s"${classOf[Validation[Any]].getName}#equalTo"), s"value doesn't equal expected value '$expectedValue': $value", value, None)
      }
    }
  }

}

/**
* Exception for validation errors.
*/
case class ValidationException(label: Option[String], message: String, value: Any, thrown: Option[Throwable]) extends Exception(if (label.isDefined) s"$label: $message" else message, thrown getOrElse null) {}
