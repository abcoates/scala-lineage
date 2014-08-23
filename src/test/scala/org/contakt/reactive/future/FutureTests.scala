package org.contakt.reactive.future

import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * Class for testing the behaviour of Scala futures.
 */
class FutureTests extends FlatSpec with Matchers {

  "A future" should "be able to be processed with the success returned in another future" in {
    val value = 5
    val future = Future{ Thread.sleep(100); value }
    assert(!future.isCompleted)
    val newPromise = Promise[Int]
    future.andThen {
      case Failure(t) => newPromise failure t
      case Success(x) => newPromise success (x * 2)
    }
    val newFuture = newPromise.future
    assert(!newFuture.isCompleted)

    val newFuture2 = for (x <- future) yield (x * 2)
    assert(!newFuture2.isCompleted)

    val newFuture3 = future map { _ * 2 }
    assert(!newFuture3.isCompleted)

    Await.ready(newFuture, Duration.Inf)
    assert(newFuture.value === Some(Success(value * 2)))

    Await.ready(newFuture2, Duration.Inf)
    assert(newFuture2.value === Some(Success(value * 2)))

    Await.ready(newFuture3, Duration.Inf)
    assert(newFuture3.value === Some(Success(value * 2)))
  }

  it should "be able to be processed with the failure returned in another future" in {
    val value = 5
    val future = Future{ Thread.sleep(100); throw new Exception() ; value }
    assert(!future.isCompleted)
    val newPromise = Promise[Int]
    future.andThen {
      case Failure(t) => newPromise failure t
      case Success(x) => newPromise success (x * 2)
    }
    val newFuture = newPromise.future
    assert(!newFuture.isCompleted)

    val newFuture2 = for (x <- future) yield (x * 2)
    assert(!newFuture2.isCompleted)

    val newFuture3 = future map { _ * 2 }
    assert(!newFuture3.isCompleted)

    Await.ready(newFuture, Duration.Inf)
    assert(newFuture.value match {
      case Some(Failure(t)) if t.isInstanceOf[Exception] => true
      case _ => false
    })

    Await.ready(newFuture2, Duration.Inf)
    assert(newFuture2.value match {
      case Some(Failure(t)) if t.isInstanceOf[Exception] => true
      case _ => false
    })

    Await.ready(newFuture3, Duration.Inf)
    assert(newFuture3.value match {
      case Some(Failure(t)) if t.isInstanceOf[Exception] => true
      case _ => false
    })
  }

  it should "be possible to test a future result using 'collect'" in {
    val value = 5
    val future = Future{ Thread.sleep(100); 5 }
    val newFuture = future collect { case 5 => 5 } // should return 5
    val newFuture2 = future collect { case 3 => 3 } // should throw 'NoSuchElementException
    assert(!newFuture.isCompleted)
    assert(!newFuture2.isCompleted)

    Await.ready(newFuture, Duration.Inf)
    assert(newFuture.value match {
      case Some(Success(5)) => true
      case _ => false
    })

    Await.ready(newFuture2, Duration.Inf)
    assert(newFuture2.value match {
      case Some(Failure(t)) => t.isInstanceOf[NoSuchElementException]
      case _ => false
    })
  }

}
