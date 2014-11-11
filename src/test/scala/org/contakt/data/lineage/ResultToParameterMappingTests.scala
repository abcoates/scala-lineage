package org.contakt.data.lineage

import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.global

/**
 * Tests for ResultToParameterMappingTests class.
 */
class ResultToParameterMappingTests extends FlatSpec with Matchers {

  val RESULTS = new ResultMap()
  RESULTS.addResult('a, Future{ 1 })
  RESULTS.addResult('b, Future{ 2 })
  RESULTS.addResult('c, Future{ 3 })

  "A result-to-parameter mapping" should "be able to map all results to parameters" in {
    val params: ParameterMap = RESULTS :: ALL
    assert(params.size === RESULTS.size)
  }

}
