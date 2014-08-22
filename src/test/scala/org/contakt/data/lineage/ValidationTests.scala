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
    val validation = NONE[Any] // note that 'NONE' needs a type, but the type can be 'Any'
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
    val value = 1
    val future = Future{ value }
    assert(!future.isCompleted)
    assert(future.value === None)
    val validation = hasResultClass[Int](classOf[java.lang.Integer]) // note that 'hasResultClass' needs a type
    val validatedFuture = validation(future)
    Await.ready(validatedFuture, Duration.Inf)
    assert(future.isCompleted)
    assert(validatedFuture.isCompleted)
    println(s"future value: ${future.value}")
    println(s"validated future value: ${validatedFuture.value}")
    assert(
      validatedFuture.value match { // check for a Some(Success(1)) value
        case Some(Success(x)) => x match {
          case int: Int if int == value => true
          case _ => false
        }
        case _ => false
      }
    )
  }

}
