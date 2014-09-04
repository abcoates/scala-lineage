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

  /** Delay at start of each future, in milliseconds. */
  val sleepDelay = 100

  def newSuccessFuture = Future{ Thread.sleep(sleepDelay); true }

  def newFailureFuture = Future{ Thread.sleep(sleepDelay); throw new Exception() }

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
    val future = Future{ Thread.sleep(sleepDelay); value }
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
    val future2 = Future{ Thread.sleep(sleepDelay); value2 }
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

  it should "be possible to validate a future's result against an expected value" in {
    val validation = equalTo(1)

    // This should succeed.
    val value = 1
    val future = Future{ Thread.sleep(sleepDelay); value }
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
    val value2 = 2
    val future2 = Future{ Thread.sleep(sleepDelay); value2 }
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
            (ve.label.get == "org.contakt.data.lineage.Validation#equalTo") &&
              (ve.value == value2) &&
              (ve.thrown.isEmpty)
          case other => println(s"fail #1: $other"); false
        }
        case other => println(s"fail #2: $other"); false
      }
    )
  }

  it should "be possible to do an 'and' between two validations" in {
    val validation1 = hasResultClass(classOf[java.lang.Integer]) // note: the Java 'Integer' type needs to be used here
    val validation2 = equalTo(1)
    val validation = validation1 && validation2

    // This should succeed.
    val value = 1
    val future = Future{ Thread.sleep(sleepDelay); value }
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
    val value2 = 2
    val future2 = Future{ Thread.sleep(sleepDelay); value2 }
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
            (ve.label.get == "org.contakt.data.lineage.Validation#equalTo") &&
              (ve.value == value2) &&
              (ve.thrown.isEmpty)
          case other => println(s"fail #1: $other"); false
        }
        case other => println(s"fail #2: $other"); false
      }
    )

    // This should fail.
    val value3 = 2.0
    val future3 = Future{ Thread.sleep(sleepDelay); value3 }
    assert(!future3.isCompleted)
    assert(future3.value === None)
    val validatedFuture3 = validation(future3)
    Await.ready(validatedFuture3, Duration.Inf)
    assert(future3.isCompleted)
    assert(validatedFuture3.isCompleted)
    assert(
      validatedFuture3.value match { // check for a Some(Failure(ValidationException(...))) value
        case Some(Failure(t)) => t match {
          case ve: ValidationException =>
            (ve.label.get == "org.contakt.data.lineage.Validation#hasResultClass") &&
              (ve.value == value3) &&
              (ve.thrown.get.isInstanceOf[ClassCastException])
          case other => println(s"fail #1: $other"); false
        }
        case other => println(s"fail #2: $other"); false
      }
    )
  }

  it should "be possible to do an 'or' between two validations" in {
    val validation1 = hasResultClass(classOf[java.lang.Integer]) // note: the Java 'Integer' type needs to be used here
    val validation2 = equalTo(1.0)
    val validation = validation1 || validation2

    // This should succeed.
    val value = 1
    val future = Future{ Thread.sleep(sleepDelay); value }
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

    // This should succeed.
    val value2 = 1.0
    val future2 = Future{ Thread.sleep(sleepDelay); value2 }
    assert(!future2.isCompleted)
    assert(future2.value === None)
    val validatedFuture2 = validation(future2)
    Await.ready(validatedFuture2, Duration.Inf)
    assert(future2.isCompleted)
    assert(validatedFuture2.isCompleted)
    assert(
      validatedFuture2.value match { // check for a Some(Success(1.0)) value
        case Some(Success(x)) => x match {
          case dbl: Double => dbl == value2
          case _ => false
        }
        case _ => false
      }
    )

    // This should fail.
    val value3 = 2.0
    val future3 = Future{ Thread.sleep(sleepDelay); value3 }
    assert(!future3.isCompleted)
    assert(future3.value === None)
    val validatedFuture3 = validation(future3)
    Await.ready(validatedFuture3, Duration.Inf)
    assert(future3.isCompleted)
    assert(validatedFuture3.isCompleted)
    assert(
      validatedFuture3.value match { // check for a Some(Failure(ValidationException(...))) value
        case Some(Failure(t)) => t match {
          case ve: ValidationException =>
            (
              (ve.label.get == "org.contakt.data.lineage.Validation#hasResultClass") ||
                (ve.label.get == "org.contakt.data.lineage.Validation#equalTo")
            ) &&
              (ve.value == value3) &&
              (ve.thrown.isEmpty || ve.thrown.get.isInstanceOf[ClassCastException])
          case other => println(s"fail #1: $other"); false
        }
        case other => println(s"fail #2: $other"); false
      }
    )
  }

}
