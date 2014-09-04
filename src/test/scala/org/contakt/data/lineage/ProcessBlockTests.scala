package org.contakt.data.lineage

import org.contakt.data.lineage.Validation._
import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await}

/**
* Tests for ProcessBlock class.
*/
class ProcessBlockTests extends FlatSpec with Matchers {

  /**
   * Creates a new, simple process block which computes the sum and difference of two integer parameters.
   * @return a new, simple process block.
   */
  def newSimpleProcessBlock = new ProcessBlock {

    /** The class for Scala 'Int' values. */
    val IntClass = classOf[java.lang.Integer]

    /**
     * Name which uniquely identifies the process block.
     * @return the name of the process block.
     */
    override def name = 'SimpleProcessBlock

    /**
     * Defines the parameters for a process block, and validations for those parameters.
     * Note: must include all parameters, including constant parameters defined by 'constParameters'.
     * @return map of name symbols and matching validations.
     */
    override def parameters = Map[String, Validation[_]]('a -> hasValueClass(IntClass), 'b -> hasValueClass(IntClass))

    /**
     * Defines the results for a process block, and validations for those results.
     * @return map of name symbols and matching validations.
     */
    override def results = Map[String, Validation[_]]('sum -> hasValueClass(IntClass), 'diff -> hasValueClass(IntClass))

    /**
     * Core processing function for the process block.
     * Note: the core processing function should <b>not</b> be called directly by users.  It is called by 'run'.
     * @return function which is run by 'run' to perform the core processing.
     */
    override def process: (ParameterMap) => ResultMap = { parameters: ParameterMap =>
      val results = new ResultMap()
      val sumResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
      results.addResult('sum, sumResult)
      val diffResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] - b.asInstanceOf[Int]) // 'for' can be used to subtract futures and return a future
      results.addResult('diff, diffResult)
      results
    }

  }

  "A process block" should "be able to be created and run" in {
    val pb = newSimpleProcessBlock
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val results = pb run parameters
    for (key <- results.keySet) {
      Await.ready(results(key), Duration.Inf)
      assert(results(key).isCompleted)
    }
    assert(results('sum).value.get.get === map('a) + map('b))
    assert(results('diff).value.get.get === map('a) - map('b))
  }

}
