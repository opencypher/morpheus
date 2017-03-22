package org.opencypher.spark.prototype.impl.syntax

import cats.data.State
import cats.data.State.{get, set}
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.record.InternalHeader
import org.opencypher.spark.prototype.impl.util.{AdditiveUpdateResult, RemovingUpdateResult}

trait RecordHeaderSyntax {

  implicit def sparkRecordHeaderSyntax(header: RecordHeader): RecordHeaderOps =
    new RecordHeaderOps(header)

  type HeaderState[X] = State[RecordHeader, X]

  def addContents(contents: Seq[SlotContent]): State[RecordHeader, Vector[AdditiveUpdateResult[RecordSlot]]] =
    exec(InternalHeader.addContents(contents))

  def addContent(content: SlotContent): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    exec(InternalHeader.addContent(content))

  def compactFields: State[RecordHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
    exec(InternalHeader.compactFields)

  def selectFields(predicate: RecordSlot => Boolean)
  : State[RecordHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
    exec(InternalHeader.selectFields(predicate))

//  def removeContent(content: SlotContent)
//  : State[RecordHeader, RemovingUpdateResult[SlotContent]] =
//    exec(InternalHeader.removeContent(content))

  private def exec[O](inner: State[InternalHeader, O]): State[RecordHeader, O] =
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
