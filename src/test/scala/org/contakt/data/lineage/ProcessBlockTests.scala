package org.contakt.data.lineage

import org.contakt.data.lineage.Validation._
import org.scalatest.{Matchers, FlatSpec}
import scala.util.{Failure, Try, Success}

/**
* Tests for ProcessBlock class.
*/
class ProcessBlockTests extends FlatSpec with Matchers {

  /** Quick test that a 'Try' is a 'Success'. */
  def isSuccess(value: Try[_]): Boolean = value match {
    case Success(x) => true
    case _ => false
  }

  /** Quick test that a 'Try' is a 'Failure'. */
  def isFailure(value: Try[_]): Boolean = value match {
    case Failure(t) => true
    case _ => false
  }

  /**
   * A simple process block for use in testing.  Computes the sum and difference of two integer parameters.
   */
  class SimpleTestProcessBlock extends ProcessBlock {

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
    val pb = new SimpleTestProcessBlock()
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val results = pb run parameters
    val resultsMap = results.awaitResults
    for (key <- results.keySet) {
      assert(results(key).isCompleted)
    }
    assert(results('sum).value.get.get === map('a) + map('b))
    assert(results('diff).value.get.get === map('a) - map('b))
  }

  it should "produce a Failure when a result fails validation" in {
    val pb = new SimpleTestProcessBlock {
      override def process: (ParameterMap) => ResultMap = { parameters: ParameterMap =>
        val results = new ResultMap()
        val sumResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
        results.addResult('sum, sumResult)
        val diffResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] - b.asInstanceOf[Int]) // 'for' can be used to subtract futures and return a future
        results.addResult('diff, diffResult.toString) // note: wrong result type
        results
      }
    }
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val results = pb run parameters
    val resultsMap = results.awaitResults
    for (key <- results.keySet) {
      assert(results(key).isCompleted)
    }
    assert(results('sum).value.get.get === map('a) + map('b))
    assert(results('diff).value match {
      case Some(Failure(t)) => // Check for a 'ResultValidationException' containing a 'ValidationException' containing a 'ClassCastException'
        if (t.isInstanceOf[ResultValidationException]) {
          val tt = t.asInstanceOf[ResultValidationException].thrown
          if (tt.isDefined && tt.get.isInstanceOf[ValidationException]) {
            val ttt = tt.get.asInstanceOf[ValidationException].thrown
            ttt.isDefined && ttt.get.isInstanceOf[ClassCastException]
          } else {
            false
          }
        } else {
          false
        }
      case _ => false
    })
  }

  // TODO: it should "produce a Failure when a parameter fails validation" in {} // TODO: should wrap a parameter exception in the result exception

  // TODO: it should "produce a Failure when an exception is throw while calculating a result in {}

  // TODO: it should "be able to use constant (pre-defined) parameters of the process block" in {}

  // TODO: it should "not allow a runtime parameter to have the same name as any constant parameter" in {}

}
