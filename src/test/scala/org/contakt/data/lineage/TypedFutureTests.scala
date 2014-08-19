package org.contakt.data.lineage

import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

/**
 * Tests for Named Future class.
 */
class TypedFutureTests extends FlatSpec with Matchers {

  "A named future" should "be able to have the future's result class interrogated" in {
    val intvalue = 3
    val intfuture = TypedFuture(Future{intvalue})
    assert(intfuture.getFutureClass === intvalue.getClass)

    val floatvalue = 2.0
    val floatfuture = TypedFuture(Future{floatvalue})
    assert(floatfuture.getFutureClass === floatvalue.getClass)

    val strvalue = "abcdef"
    val strfuture = TypedFuture(Future{strvalue})
    assert(strfuture.getFutureClass === strvalue.getClass)
  }

  it should "be possible to await the result of a named future" in {
    val intvalue = 3
    val intfuture = TypedFuture(Future{Thread.sleep(10); intvalue})
    assert(!intfuture.isCompleted)
    assert(intfuture.value === None)
    intfuture.awaitReady
    assert(intfuture.isCompleted)
    assert(intfuture.value == Some(Success(intvalue)))

    val floatvalue = 2.0
    val floatfuture = TypedFuture(Future{floatvalue})
    val awaitedvalue = floatfuture.awaitResult
    assert(awaitedvalue === floatvalue)
  }

}
