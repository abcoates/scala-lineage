package org.contakt.data.lineage

import org.contakt.data.lineage._
import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
* Tests for ParameterMap class.
*/
class ParameterMapTests extends FlatSpec with Matchers {

  "A value" should "be auto-converted to a future as required" in {
    val f1: Future[_] = 1
    val f2: Future[_] = 1.0
    val f3: Future[_] = true
    val f4: Future[_] = 'a'
    val f5: Future[_] = "b"
  }

  "A parameter map" should "be constructable from a map" in {
    val pmap0 = new ParameterMap(Map[String, Any]())
    pmap0.size should be (0)
    val pmap1 = new ParameterMap(Map[String, Any]('a -> 1))
    pmap1.size should be (1)
    val pmap2 = new ParameterMap(Map[String, Any]('a -> 1, 'b -> 2.0))
    pmap2.size should be (2)
    val pmap3 = new ParameterMap(Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three"))
    pmap3.size should be (3)
    val pmap4 = new ParameterMap(Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4)))
    pmap4.size should be (4)
  }

  it should "have non-future values converted to futures" in {
    val map = Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4))
    val fmap: Map[String, Future[Any]] = map.map{mapping => mapping._1 -> Future{ mapping._2 }}
    val pmap = new ParameterMap(map)
    for (key <- pmap.keySet) {
      val fvalue = fmap(key)
      val pvalue = pmap(key)
      fvalue shouldBe a [Future[Any]]
      pvalue shouldBe a [Future[Any]]
    }
  }

  it should "return the parameter value for a given name symbol" in {
    val map = Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4))
    val pmap = new ParameterMap(map)
    for (key <- pmap.keySet) {
      Await.ready(pmap(key), Duration.Inf)
      assert(pmap(key).value.get === Success(map(key)))
      assert(pmap.tryParameter(key).get.value.get === Success(map(key)))
    }
  }

  it should "return a failed future for a name symbol with no matching parameter value" in {
    val map = Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4))
    val pmap = new ParameterMap(map)
    val value = pmap.tryParameter('e)
    Await.ready(value, Duration.Inf)
    assert(value match {
      case Failure(t) => t.isInstanceOf[NoSuchElementException]
      case other => false
    })
  }

  it should "be possible to aggregate two parameter maps" in {
    val map1 = Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4))
    val pmap1 = new ParameterMap(map1)
    val map2 = Map[String, Any]('e -> 1, 'f -> 2.0, 'g -> "three", 'h -> List(4))
    val pmap2 = new ParameterMap(map2)
    val combinedMap = pmap1 ++ pmap2
    for (key <- pmap1.keySet) {
      Await.ready(pmap1(key), Duration.Inf)
      assert(pmap1(key).isCompleted)
      assert(combinedMap(key).isCompleted)
      assert(pmap1(key).value === combinedMap(key).value)
    }
    for (key <- pmap2.keySet) {
      Await.ready(pmap2(key), Duration.Inf)
      assert(pmap2(key).isCompleted)
      assert(combinedMap(key).isCompleted)
      assert(pmap2(key).value === combinedMap(key).value)
    }
  }

  it should "create Failure values if two sets with overlapping parameter names are aggregated" in {
    val map1 = Map[String, Any]('a -> 1, 'b -> 2.0, 'c -> "three", 'd -> List(4))
    val pmap1 = new ParameterMap(map1)
    val map2 = Map[String, Any]('c -> 1, 'd -> 2.0, 'e -> "three", 'f -> List(4))
    val pmap2 = new ParameterMap(map2)
    val combinedMap = pmap1 ++ pmap2
    for (key <- pmap1.keySet if !pmap2.isDefinedAt(key)) {
      Await.ready(pmap1(key), Duration.Inf)
      assert(pmap1(key).isCompleted)
      assert(combinedMap(key).isCompleted)
      assert(pmap1(key).value === combinedMap(key).value)
    }
    for (key <- pmap2.keySet if !pmap1.isDefinedAt(key)) {
      Await.ready(pmap2(key), Duration.Inf)
      assert(pmap2(key).isCompleted)
      assert(combinedMap(key).isCompleted)
      assert(pmap2(key).value === combinedMap(key).value)
    }
    for (key <- pmap1.keySet if pmap2.isDefinedAt(key)) {
      Await.ready(combinedMap(key), Duration.Inf)
      assert(combinedMap(key).isCompleted)
      assert(combinedMap(key).value.get match {
        case Failure(t) => t.isInstanceOf[DuplicatedParameterNameException]
        case other => false
      })
    }
  }

}
