package org.contakt.data.lineage

import org.contakt.data.lineage.Validation._
import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * Tests for the Validation trait.
 */
class ValidationTests extends FlatSpec with Matchers {

  def newSuccessFuture = Future{ Thread.sleep(100); true }

  def newFailureFuture = Future{ Thread.sleep(100); throw new Exception() }

  "A new success future" should "be able to be created" in {
    val future = newSuccessFuture
    assert(!future.isCompleted)
    assert(future.value === None)
    Await.ready(future, Duration.Inf)
    assert(future.isCompleted)
    assert(
      future.value match { // check for a Some(Success(true)) value
        case Some(Success(x)) => x match {
          case bool: Boolean if bool == true => true
          case _ => false
        }
        case _ => false
      }
    )
  }

  "A new failure future" should "be able to be created" in {
    val future = newFailureFuture
    assert(!future.isCompleted)
    assert(future.value === None)
    Await.ready(future, Duration.Inf)
    assert(future.isCompleted)
    assert(
      future.value match { // check for a Some(Failure(Exception)) value
        case Some(Failure(t)) => t match {
          case ex: Exception => true
          case _ => false
        }
        case _ => false
      }
    )
  }

  "A validation" should "be able to be a pass-through 'identity' validation that doesn't validate anything" in {
    val future = newSuccessFuture
    assert(!future.isCompleted)
    assert(future.value === None)
    val validation = NONE
    val validatedFuture = validation(future)
    assert(validatedFuture eq future)
    Await.ready(validatedFuture, Duration.Inf)
    assert(future.isCompleted)
    assert(validatedFuture.isCompleted)
    assert(
      validatedFuture.value match { // check for a Some(Success(true)) value
        case Some(Success(x)) => x match {
          case bool: Boolean if bool == true => true
          case _ => false
        }
        case _ => false
      }
    )
  }

  it should "be possible to validate a future's result against an expected class" in {
    val validation = hasResultClass(classOf[java.lang.Integer]) // note: the Java 'Integer' type needs to be used here

    // This should succeed.
    val value = 1
    val future = Future{ value }
    assert(!future.isCompleted)
    assert(future.value === None)
    val validatedFuture = validation(future)
    Await.ready(validatedFuture, Duration.Inf)
    assert(future.isCompleted)
    assert(validatedFuture.isCompleted)
    assert(
      validatedFuture.value match { // check for a Some(Success(1)) value
        case Some(Success(x)) => x match {
          case int: Int => int == value
          case _ => false
        }
        case _ => false
      }
    )

    // This should fail.
    val value2 = 2.0
    val future2 = Future{ value2 }
    assert(!future2.isCompleted)
    assert(future2.value === None)
    val validatedFuture2 = validation(future2)
    Await.ready(validatedFuture2, Duration.Inf)
    assert(future2.isCompleted)
    assert(validatedFuture2.isCompleted)
    assert(
      validatedFuture2.value match { // check for a Some(Failure(ValidationException(...))) value
        case Some(Failure(t)) => t match {
          case ve: ValidationException =>
            (ve.label.get == "org.contakt.data.lineage.Validation#hasResultClass") &&
              (ve.value == value2) &&
              (ve.thrown.get.isInstanceOf[ClassCastException])
          case other => println(s"fail #1: $other"); false
        }
        case other => println(s"fail #2: $other"); false
      }
    )
  }

}
