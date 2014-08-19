package org.contakt.data.lineage

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * A Future where the type of the future is captured using a class tag.
 * Also provides shorthands for calling 'Await' methods.
 */
case class TypedFuture[T](val future: Future[T])(implicit manifest: Manifest[T]) {

  def getFutureClass: Class[_] = manifest.runtimeClass

  def isCompleted: Boolean = future.isCompleted

  def value: Option[Try[T]] = future.value

  def awaitReady(atMost: Duration): Unit = Await.ready[T](future, atMost)

  def awaitReady: Unit = awaitReady(Duration.Inf)

  def awaitResult(atMost: Duration): T = Await.result[T](future, atMost)

  def awaitResult: T = awaitResult(Duration.Inf)

}
