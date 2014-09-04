package org.contakt.data.lineage

import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Success, Failure}

/**
* Trait for a function that can be used to validate a future, returning the same future is successful, or a Future(Failure(...)) if validation fails.
*/
abstract class Validation[T](label: Option[String] = None)(implicit executionContext: ExecutionContext) extends Function1[Future[T], Future[T]] {

  /** Used to refer to the correct 'this' when returning functions. */
  private val thisValidation = this
  
  /**
   * Combines two validations via an 'and' operation.
   * @param vld validation to 'and' with this validation.
   * @return new 'and' validation.
   */
  def &&(vld: Validation[T]) = new Validation[T] {
    
    def apply(future: Future[T]): Future[T] = {
      val newPromise = Promise[T]
      future.andThen {
        case Failure(t) => newPromise failure t
        case Success(x) => thisValidation(future).andThen {
          case Failure(t) => newPromise failure t
          case Success(x) => vld(future).andThen {
            case Failure(t) => newPromise failure t
            case Success(x) => newPromise success x
          }
        }
      }
      newPromise.future
    }

  }

  /**
   * Combines two validations via an 'or' operation.
   * @param vld validation to 'or' with this validation.
   * @return new 'or' validation.
   */
  def ||(vld: Validation[T]) = new Validation[T] {

    def apply(future: Future[T]): Future[T] = {
      val newPromise = Promise[T]
      future.andThen {
        case Failure(t) => newPromise failure t
        case Success(x) => thisValidation(future).andThen {
          case Success(x) => newPromise success x
          case Failure(t) => vld(future).andThen {
            case Failure(t) => newPromise failure t
            case Success(x) => newPromise success x
          }
        }
      }
      newPromise.future
    }

  }

}

/**
* Companion object for Validation trait.
*/
object Validation {

  /** A pass-through validation that doesn't check anything. */
  def NONE(implicit executionContext: ExecutionContext) = new Validation[Any] {
    override def apply(future: Future[Any]) = future
  }

  /** This validation checks that the result of the future can be cast to the given class. */
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

  /** This validation checks that the result of the future has a particular value. */
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
