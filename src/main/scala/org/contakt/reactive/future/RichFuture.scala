package org.contakt.reactive.future

import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.{Success, Failure, Try}

/**
 * Class that adds convenience methods for futures.
 */
class RichFuture[T](val future: Future[T]) {

  /** Like 'collect', but works with the future's 'Try' value directly. */
  def collectValue(pf: PartialFunction[Try[T], Try[T]])(implicit executionContext: ExecutionContext): Future[T] = {
    val newPromise = Promise[T]
    future onComplete { value =>
      pf(value) match {
        case Success(x) => newPromise success x
        case Failure(t) => newPromise failure t
      }
    }
    newPromise.future
  }

}
