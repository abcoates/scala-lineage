package org.contakt.data.lineage

import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Tests for ResultToParameterMappingTests class.
 */
class ResultToParameterMappingTests extends FlatSpec with Matchers {

  val RESULTS = new ResultMap()
  RESULTS.addResult('a, Future{ 1 })
  RESULTS.addResult('b, Future{ 2 })
  RESULTS.addResult('c, Future{ 3 })
  RESULTS.addResult('d, Future{ 4 })
  RESULTS.addResult('e, Future{ 5 })

  "A result-to-parameter mapping" should "be able to map all results to parameters" in {
    val params: ParameterMap = RESULTS :: DEFAULT_MAP_ALL
    assert(params.size === RESULTS.size, s"wrong number of results: expected ${RESULTS.size}, found ${params.size}")
  }

  it should "be able to map no results to parameters" in {
    val params: ParameterMap = RESULTS :: DEFAULT_MAP_NONE
    assert(params.size === 0, s"expected no results, found ${params.size}")
  }

  it should "be possible to map a single result name to a different parameter name" in {
    val params: ParameterMap = RESULTS :: ("a" -> "f") :: DEFAULT_MAP_ALL
    assert(params.size === RESULTS.size, s"wrong number of results: expected ${RESULTS.size}, found ${params.size}")
    assert(!params.isDefinedAt("a"), "mapping failed, found unexpected 'a' parameter")
    assert(params.isDefinedAt("b"), "mapping failed, did not find expected 'b' parameter")
    assert(params.isDefinedAt("c"), "mapping failed, did not find expected 'c' parameter")
    assert(params.isDefinedAt("d"), "mapping failed, did not find expected 'd' parameter")
    assert(params.isDefinedAt("e"), "mapping failed, did not find expected 'e' parameter")
    assert(params.isDefinedAt("f"), "mapping failed, did not find expected 'f' parameter")
  }

  it should "be possible to map multiple result names to different parameter names" in {
    val params: ParameterMap = RESULTS :: Map("a" -> "f", "b" -> "g") :: DEFAULT_MAP_ALL
    assert(params.size === RESULTS.size, s"wrong number of results: expected ${RESULTS.size}, found ${params.size}")
    assert(!params.isDefinedAt("a"), "mapping failed, found unexpected 'a' parameter")
    assert(!params.isDefinedAt("b"), "mapping failed, found unexpected 'b' parameter")
    assert(params.isDefinedAt("c"), "mapping failed, did not find expected 'c' parameter")
    assert(params.isDefinedAt("d"), "mapping failed, did not find expected 'd' parameter")
    assert(params.isDefinedAt("e"), "mapping failed, did not find expected 'e' parameter")
    assert(params.isDefinedAt("f"), "mapping failed, did not find expected 'f' parameter")
    assert(params.isDefinedAt("g"), "mapping failed, did not find expected 'g' parameter")
  }

  it should "be possible to map a single result name to the same parameter name" in {
    val params: ParameterMap = RESULTS :: "a" :: DEFAULT_MAP_NONE
    assert(params.size === 1, s"expected 1 results, found ${params.size}")
    assert(params.isDefinedAt("a"), "mapping failed, did not find expected 'a' parameter")
  }

  it should "be possible to map result names to the same parameter names" in {
    val params: ParameterMap = RESULTS :: List("a","b") :: DEFAULT_MAP_NONE
    assert(params.size === 2, s"expected 2 results, found ${params.size}")
    assert(params.isDefinedAt("a"), "mapping failed, did not find expected 'a' parameter")
    assert(params.isDefinedAt("b"), "mapping failed, did not find expected 'b' parameter")
  }

  it should "be possible to compose single-name mappings" in {
    val params: ParameterMap = RESULTS :: ("a" -> "f") :: ("b" -> "g") :: DEFAULT_MAP_ALL
    assert(params.size === RESULTS.size, s"wrong number of results: expected ${RESULTS.size}, found ${params.size}")
    assert(!params.isDefinedAt("a"), "mapping failed, found unexpected 'a' parameter")
    assert(!params.isDefinedAt("b"), "mapping failed, found unexpected 'b' parameter")
    assert(params.isDefinedAt("c"), "mapping failed, did not find expected 'c' parameter")
    assert(params.isDefinedAt("d"), "mapping failed, did not find expected 'd' parameter")
    assert(params.isDefinedAt("e"), "mapping failed, did not find expected 'e' parameter")
    assert(params.isDefinedAt("f"), "mapping failed, did not find expected 'f' parameter")
    assert(params.isDefinedAt("g"), "mapping failed, did not find expected 'g' parameter")
  }

  it should "be possible to compose multi-name mappings" in {
    val params: ParameterMap = RESULTS :: Map("a" -> "f", "b" -> "g") :: Map("c" -> "h", "d" -> "j") :: DEFAULT_MAP_ALL
    assert(params.size === RESULTS.size, s"wrong number of results: expected ${RESULTS.size}, found ${params.size}")
    assert(!params.isDefinedAt("a"), "mapping failed, found unexpected 'a' parameter")
    assert(!params.isDefinedAt("b"), "mapping failed, found unexpected 'b' parameter")
    assert(!params.isDefinedAt("c"), "mapping failed, found unexpected 'c' parameter")
    assert(!params.isDefinedAt("d"), "mapping failed, found unexpected 'd' parameter")
    assert(params.isDefinedAt("e"), "mapping failed, did not find expected 'e' parameter")
    assert(params.isDefinedAt("f"), "mapping failed, did not find expected 'f' parameter")
    assert(params.isDefinedAt("g"), "mapping failed, did not find expected 'g' parameter")
    assert(params.isDefinedAt("h"), "mapping failed, did not find expected 'h' parameter")
    assert(params.isDefinedAt("j"), "mapping failed, did not find expected 'j' parameter")
  }

  it should "be possible to compose a single name and a multi-name mapping" in {
    val params: ParameterMap = RESULTS :: ("a" -> "f") :: Map("b" -> "g", "c" -> "h") :: DEFAULT_MAP_ALL
    assert(params.size === RESULTS.size, s"[1] wrong number of results: expected ${RESULTS.size}, found ${params.size}")
    assert(!params.isDefinedAt("a"), "[1] mapping failed, found unexpected 'a' parameter")
    assert(!params.isDefinedAt("b"), "[1] mapping failed, found unexpected 'b' parameter")
    assert(!params.isDefinedAt("c"), "[1] mapping failed, found unexpected 'c' parameter")
    assert(params.isDefinedAt("d"), "[1] mapping failed, did not find expected 'd' parameter")
    assert(params.isDefinedAt("e"), "[1] mapping failed, did not find expected 'e' parameter")
    assert(params.isDefinedAt("f"), "[1] mapping failed, did not find expected 'f' parameter")
    assert(params.isDefinedAt("g"), "[1] mapping failed, did not find expected 'g' parameter")
    assert(params.isDefinedAt("h"), "[1] mapping failed, did not find expected 'h' parameter")

    val params2: ParameterMap = RESULTS :: Map("a" -> "f", "b" -> "g") :: ("c" -> "h") :: DEFAULT_MAP_ALL
    assert(params2.size === RESULTS.size, s"[2] wrong number of results: expected ${RESULTS.size}, found ${params2.size}")
    assert(!params2.isDefinedAt("a"), "[2] mapping failed, found unexpected 'a' parameter")
    assert(!params2.isDefinedAt("b"), "[2] mapping failed, found unexpected 'b' parameter")
    assert(!params2.isDefinedAt("c"), "[2] mapping failed, found unexpected 'c' parameter")
    assert(params2.isDefinedAt("d"), "[2] mapping failed, did not find expected 'd' parameter")
    assert(params2.isDefinedAt("e"), "[2] mapping failed, did not find expected 'e' parameter")
    assert(params2.isDefinedAt("f"), "[2] mapping failed, did not find expected 'f' parameter")
    assert(params2.isDefinedAt("g"), "[2] mapping failed, did not find expected 'g' parameter")
    assert(params2.isDefinedAt("h"), "[2] mapping failed, did not find expected 'h' parameter")
  }

}
