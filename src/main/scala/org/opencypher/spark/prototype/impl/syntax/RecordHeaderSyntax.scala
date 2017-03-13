package org.opencypher.spark.prototype.impl.syntax

import cats.{Monad, MonadState}
import cats.data.State
import cats.data.State.{get, set}
import cats.instances.all._
import cats.syntax.all._
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.record.InternalHeader
import org.opencypher.spark.prototype.impl.util.AdditiveUpdateResult

trait RecordHeaderSyntax {

  implicit def sparkRecordHeaderSyntax(header: RecordHeader): RecordHeaderOps =
    new RecordHeaderOps(header)

  type HeaderState[X] = State[RecordHeader, X]

  def addContents(contents: Seq[SlotContent]): State[RecordHeader, Vector[AdditiveUpdateResult[RecordSlot]]] = {
    val input = contents.map(addContent).toVector
    Monad[HeaderState].sequence(input)
  }

  def addContent(content: SlotContent): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    add(InternalHeader.addContent(content))

  def addProjectedExpr(content: ProjectedExpr): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    add(InternalHeader.addProjectedExpr(content))

  def addOpaqueField(content: OpaqueField): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    add(InternalHeader.addOpaqueField(content))

  def addProjectedField(content: ProjectedField): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    add(InternalHeader.addProjectedField(content))

  private def add[O](inner: State[InternalHeader, O]): State[RecordHeader, O] =
    get[RecordHeader]
    .map(header => inner.run(header.internalHeader).value)
    .flatMap { case (newInternalHeader, value) => set(RecordHeader(newInternalHeader)).map(_ => value) }
}

final class RecordHeaderOps(header: RecordHeader) {

  def +(content: SlotContent): RecordHeader =
    header.copy(header.internalHeader + content)

  def update[A](ops: State[RecordHeader, A]): (RecordHeader, A) =
    ops.run(header).value
}
