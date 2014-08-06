package org.contakt.data.lineage

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.{Matchers, FlatSpec}

import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success}

/**
 * Tests for ParameterMap class.
 */
class ParameterMapTests extends FlatSpec with Matchers {

  "A parameter map" should "be constructable from a map" in {
    val pmap0 = new ParameterMap(Map[Symbol, Any]())
    pmap0.size should be (0)
    val pmap1 = new ParameterMap(Map('a -> 1))
    pmap1.size should be (1)
    val pmap2 = new ParameterMap(Map('a -> 1, 'b -> 2.0))
    pmap2.size should be (2)
    val pmap3 = new ParameterMap(Map('a -> 1, 'b -> 2.0, 'c -> "three"))
    pmap3.size should be (3)
    val pmap4 = new ParameterMap(Map('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4)))
    pmap4.size should be (4)
  }

  it should "be implicitly constructable from a map" in {
    val pmap0: ParameterMap = Map[Symbol, Any]()
    pmap0.size should be (0)
    val pmap1: ParameterMap = Map('a -> 1)
    pmap1.size should be (1)
    val pmap2: ParameterMap = Map('a -> 1, 'b -> 2.0)
    pmap2.size should be (2)
    val pmap3: ParameterMap = Map('a -> 1, 'b -> 2.0, 'c -> "three")
    pmap3.size should be (3)
    val pmap4: ParameterMap = Map('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4))
    pmap4.size should be (4)
  }

  it should "return the Success value from a successful completed Future wrapped in a Success" in {
    val map = Map('a -> Success(Future{ 1 + 2 })) // value is a future that will complete almost immediately
    val pmap = new ParameterMap(map)
    pmap.size should be (1)
    blocking {
      Thread sleep 10/*ms*/
    }
    map('a).get.isCompleted should be (true)
    val param = pmap tryParameter 'a
    assert(param === Success(3))
  }

  it should "return the Failure value from a failed completed Future wrapped in a Success" in {
    val map = Map('a -> Success(Future{ 1 / 0 })) // value is a future that will complete (fail) almost immediately
    val pmap = new ParameterMap(map)
    pmap.size should be (1)
    blocking {
      Thread sleep 10/*ms*/
    }
    map('a).get.isCompleted should be (true)
    val param = pmap tryParameter 'a
    param shouldBe a [Failure[Exception]]
  }

  it should "return an uncompleted Future in a Success unchanged" in {
    val map = Map('a -> Success(Future{ Thread.sleep(10/*ms*/) ; 1 + 2 })) // value is a future that will complete after approximately 10ms.
    val pmap = new ParameterMap(map)
    pmap.size should be (1)
    map('a).get.isCompleted should be (false)
    val param = pmap tryParameter 'a
    assert(pmap.tryParameter('a) === map('a))
    map('a).get.isCompleted should be (false)
  }

  it should "pass through any Try value unchanged when 'tryParameter' is called, excluding a Success(Failure(...)) value" in {
    val map = Map('a -> Success(1), 'b -> Failure(new Exception()))
    val pmap = new ParameterMap(map)
    pmap.size should be (2)
    for (key <- map.keySet) {
      assert(pmap.tryParameter(key) === map(key))
    }
  }

  it should "return the Success value from a successful completed Future" in {
    val map = Map('a -> Future{ 1 + 2 }) // value is a future that will complete almost immediately
    val pmap = new ParameterMap(map)
    pmap.size should be (1)
    blocking {
      Thread sleep 10/*ms*/
    }
    map('a).isCompleted should be (true)
    val param = pmap tryParameter 'a
    assert(param === Success(3))
  }

  it should "return the Failure value from a failed completed Future" in {
    val map = Map('a -> Future{ 1 / 0 }) // value is a future that will complete (fail) almost immediately
    val pmap = new ParameterMap(map)
    pmap.size should be (1)
    blocking {
      Thread sleep 10/*ms*/
    }
    map('a).isCompleted should be (true)
    val param = pmap tryParameter 'a
    param shouldBe a [Failure[Exception]]
  }

  it should "return an uncompleted Future wrapped in a Success" in {
    val map = Map('a -> Future{ Thread.sleep(10/*ms*/) ; 1 + 2 }) // value is a future that will complete after approximately 10ms.
    val pmap = new ParameterMap(map)
    pmap.size should be (1)
    map('a).isCompleted should be (false)
    val param = pmap tryParameter 'a
    param shouldBe a [Success[Future[Any]]]
    map('a).isCompleted should be (false)
  }

  it should "return a throwable, exception or null from 'tryParameter' as a Failure" in {
    val map = Map('a -> new Throwable(), 'b -> new Exception(), 'c -> null)
    val pmap = new ParameterMap(map)
    pmap.size should be (3)
    for (key <- map.keySet) {
      map(key) match {
        case null => pmap.tryParameter(key) shouldBe a [Failure[Exception]]
        case t: Throwable => assert(pmap.tryParameter(key) === Failure(t))
      }
    }
  }

  it should "return a Success value when 'tryParameter' is used to retrieve any other kind of value" in {
    val map = Map(
      'a -> 1.toShort,
      'b -> 1,
      'c -> 1l,
      'd -> 1.0f,
      'e -> 1.0d,
      'f -> true,
      'g -> 'f',
      'h -> "g",
      'i -> List(1,2,3),
      'j -> Tuple3(1, 'b, true)
    )
    val pmap = new ParameterMap(map)
    pmap.size should be (10)
    for (key <- map.keySet) {
      assert(pmap.tryParameter(key) === Success(map(key)))
    }
  }

}
