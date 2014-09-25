package org.contakt.data.lineage

import org.contakt.data.lineage.Validation._
import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Success, Failure}

/**
* Tests for ProcessBlock class.
*/
class ProcessBlockTests extends FlatSpec with Matchers {
  
  val futurePauseMsec = 100

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
    override def parameterChecks = parameterCheckMap
    private val parameterCheckMap = Map[String, Validation[_]]('a -> hasValueClass(IntClass), 'b -> hasValueClass(IntClass))

    /**
     * Defines the results for a process block, and validations for those results.
     * @return map of name symbols and matching validations.
     */
    override def resultChecks = resultCheckMap
    private val resultCheckMap = Map[String, Validation[_]]('sum -> hasValueClass(IntClass), 'diff -> hasValueClass(IntClass))

    /**
     * Core processing function for the process block.
     * Note: the core processing function should <b>not</b> be called directly by users.  It is called by 'run'.
     * @return function which is run by 'run' to perform the core processing.
     */
    override def process: (ParameterMap) => ResultMap = { parameters: ParameterMap =>
      val processResults = new ResultMap()
      val sumResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
      processResults.addResult('sum, sumResult)
      val diffResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] - b.asInstanceOf[Int]) // 'for' can be used to subtract futures and return a future
      processResults.addResult('diff, diffResult)
      processResults
    }

  }

  "A process block" should "be able to be created and run" in {
    val pb = new SimpleTestProcessBlock()
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.get.get === map('a) + map('b))
    assert(runResults('diff).value.get.get === map('a) - map('b))
  }

  it should "produce a Failure when a result fails validation" in {
    val pb = new SimpleTestProcessBlock {
      override def process: (ParameterMap) => ResultMap = { parameters: ParameterMap =>
        val results = new ResultMap()
        val sumResult: Future[Int] = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
        results.addResult('sum, sumResult)
        val diffResult: Future[String] = for (a <- parameters('a); b <- parameters('b)) yield ((a.asInstanceOf[Int] - b.asInstanceOf[Int])).toString // 'for' can be used to subtract futures and return a future
        results.addResult('diff, diffResult) // note: wrong result type - String, not Int
        results
      }
    }
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.get.get === map('a) + map('b))
    assert(runResults('diff).value.isDefined && runResults('diff).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('diff),
      List(classOf[ResultValidationException], classOf[ValidationException], classOf[ClassCastException])
    ))
  }

  it should "produce a Failure when a parameter fails validation" in {
    val pb = new SimpleTestProcessBlock()
    val map = Map[String, Any](
      'a -> 3,
      'b -> 2.0 // note: not an Int as expected
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.isDefined && runResults('sum).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('sum),
      List(classOf[ResultValidationException], classOf[ParameterValidationException], classOf[ValidationException], classOf[ClassCastException])
    ))
    assert(runResults('diff).value.isDefined && runResults('diff).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('diff),
      List(classOf[ResultValidationException], classOf[ParameterValidationException], classOf[ValidationException], classOf[ClassCastException])
    ))
  }

  it should "produce a Failure when an exception is throw while calculating a result" in {
    val exceptionLabel = "LABEL0001"
    val pb = new SimpleTestProcessBlock {
      override def process: (ParameterMap) => ResultMap = { parameters: ParameterMap =>
        val results = new ResultMap()
        val sumResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
        results.addResult('sum, sumResult)
        val diffResult = Future { // using an explicit Future here to make sure the Exception is passed through as a Failure as expected
          Thread.sleep(futurePauseMsec)
          throw new Exception(s"$exceptionLabel - test exception to check process block behaviour")
          (parameters('a).asInstanceOf[Int] -  parameters('b).asInstanceOf[Int])
        }
        results.addResult('diff, diffResult) // note: should be a failure due to the exception
        results
      }
    }
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.get.get === map('a) + map('b))
    assert(runResults('diff).value.isDefined && runResults('diff).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('diff),
      List(classOf[ResultValidationException], classOf[Exception])
    ))
    assert(runResults('diff).value.get.asInstanceOf[Failure[_]].exception.getCause.getMessage.startsWith(exceptionLabel))
  }

  it should "be able to use constant (pre-defined) parameters of the process block" in {
    val constMap = Map[String, Int](
      'b -> 2
    )
    val pb = new SimpleTestProcessBlock{
      override def constParameters = new ParameterMap(constMap)(executionContext)
    }
    val map = Map[String, Int](
      'a -> 3
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.get.get === map('a) + constMap('b))
    assert(runResults('diff).value.get.get === map('a) - constMap('b))
  }

  it should "not allow a runtime parameter to have the same name as any constant parameter" in {
    val constMap = Map[String, Int](
      'b -> 2
    )
    val pb = new SimpleTestProcessBlock{
      override def constParameters = new ParameterMap(constMap)(executionContext)
    }
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2 // uh-oh, same name as a constant parameter!
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.isDefined && runResults('sum).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('sum),
      List(classOf[ResultValidationException], classOf[ParameterValidationException], classOf[DuplicatedParameterNameException])
    ))
    assert(runResults('diff).value.isDefined && runResults('diff).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('diff),
      List(classOf[ResultValidationException], classOf[ParameterValidationException], classOf[DuplicatedParameterNameException])
    ))
  }

  it should "not allow two results to have the same name" in {
    val pb = new SimpleTestProcessBlock {
      override def process: (ParameterMap) => ResultMap = { parameters: ParameterMap =>
        val results = new ResultMap()
        val sumResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
        results.addResult('sum, sumResult)
        val diffResult = for (a <- parameters('a); b <- parameters('b)) yield (a.asInstanceOf[Int] - b.asInstanceOf[Int]) // 'for' can be used to subtract futures and return a future
        results.addResult('diff, diffResult) // note: wrong result type - String, not Int
        val doubleSumResult = for (a <- parameters('a); b <- parameters('b)) yield 2*(a.asInstanceOf[Int] + b.asInstanceOf[Int]) // 'for' can be used to add futures and return a future
        results.addResult('sum, doubleSumResult) // uh-oh, same result name twice!
        results
      }
    }
    val map = Map[String, Int](
      'a -> 3,
      'b -> 2
    )
    val parameters = new ParameterMap(map)(pb.executionContext)
    val runResults = pb run parameters
    val resultsMap = runResults.awaitResults
    for (key <- runResults.keySet) {
      assert(runResults(key).isCompleted)
    }
    assert(runResults('sum).value.isDefined && runResults('sum).value.get.isFailure)
    assert(checkExceptionChain(
      runResults('sum),
      List(classOf[ResultValidationException], classOf[DuplicatedResultNameException])
    ))
    val sum: Int = map('a) + map('b)
    assert(runResults('sum).value.get.asInstanceOf[Failure[_]].exception.getCause.asInstanceOf[DuplicatedResultNameException].oldValue.value === Some(Success(sum)))
    assert(runResults('sum).value.get.asInstanceOf[Failure[_]].exception.getCause.asInstanceOf[DuplicatedResultNameException].newValue.value === Some(Success(2*sum)))
    assert(runResults('diff).value.get.get === map('a) - map('b))
  }

}
