package org.contakt.data.lineage

import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

/**
* Tests for ResultMap class.
*/
class ResultMapTests extends FlatSpec with Matchers {

  "A result map" should "be able to have values added" in {
    val rmap = new ResultMap()
    rmap addResult ('a, 1)
    rmap addResult ('b, 2.0)
    rmap addResult ('c, 'c')
    rmap addResult ('d, "d")
    rmap addResult ('e, false)
    rmap.size should be (5)
  }

  it should "not allow the same named result to be added twice" in {
    val rmap = new ResultMap()
    rmap addResult ('a, 1)
    rmap.size should be (1)
    assert(rmap isDefinedAt 'a)
    rmap addResult ('a, 2)
    rmap.awaitResults
    assert(rmap('a).value match {
      case Some(Failure(t)) if t.isInstanceOf[DuplicatedResultNameException] =>
        val tdrne = t.asInstanceOf[DuplicatedResultNameException]
        (tdrne.oldValue.value == Some(Success(1))) &&
          (tdrne.newValue.value == Some(Success(2)))
      case _ => false
    })
  }

  it should "be able to return the result for a given name" in {
    val rmap = new ResultMap()
    val value = 1
    rmap addResult ('a, value)
    rmap.size should be (1)
    val rvalue = Await.result(rmap('a), Duration.Inf)
    assert(rvalue === value)
  }

  it should "fail to return a result for a name that doesn't have a matching result" in {
    val rmap = new ResultMap()
    val value = 1
    rmap addResult ('a, value)
    rmap.size should be (1)
    assert(rmap tryGetResult 'b match {
      case Failure(t) => t.isInstanceOf[NoSuchElementException]
      case other => false
    })
  }

  it should "be able to return its results in a map" in {
    val rmap = new ResultMap()
    rmap addResult ('a, 1)
    rmap addResult ('b, 2.0)
    rmap addResult ('c, 'c')
    rmap addResult ('d, "d")
    rmap addResult ('e, false)
    rmap.size should be (5)
    val resultsMap = rmap.getResults
    assert(resultsMap.isInstanceOf[Map[String, Future[_]]])
    for (key <- rmap.keySet) {
      assert(resultsMap(key) === rmap(key))
    }
  }

  it should "be able to return the completed future results in a map" in {
    val map = Map[String, Any](
      'a -> 1,
      'b -> 2.0,
      'c -> 'c',
      'd -> "d",
      'e -> false
    )
    val rmap = new ResultMap()
    for (key <- map.keySet) { rmap addResult (key, map(key)) }
    rmap.size should be (5)
    val resultsMap = rmap.awaitResults
    for (key <- rmap.keySet) {
      assert(rmap(key).isCompleted)
      assert(resultsMap(key) === Success(map(key)))
    }
  }

}
