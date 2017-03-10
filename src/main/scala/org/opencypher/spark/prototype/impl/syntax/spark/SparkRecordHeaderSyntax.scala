package org.opencypher.spark.prototype.impl.syntax.spark

import cats.data.State
import cats.data.State.{get, set}
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.spark.SparkInternalHeader
import org.opencypher.spark.prototype.impl.util.AdditiveUpdateResult
import org.opencypher.spark.prototype.api.spark.SparkRecordsHeader

trait SparkRecordHeaderSyntax {
  implicit def sparkRecordHeaderSyntax(header: SparkRecordsHeader): SparkRecordHeaderOps =
    new SparkRecordHeaderOps(header)

  def addContent(content: SlotContent): State[SparkRecordsHeader, AdditiveUpdateResult[RecordSlot]] =
    add(SparkInternalHeader.addContent(content))

  def addProjectedExpr(content: ProjectedExpr): State[SparkRecordsHeader, AdditiveUpdateResult[RecordSlot]] =
    add(SparkInternalHeader.addProjectedExpr(content))

  def addOpaqueField(content: OpaqueField): State[SparkRecordsHeader, AdditiveUpdateResult[RecordSlot]] =
    add(SparkInternalHeader.addOpaqueField(content))

  def addProjectedField(content: ProjectedField): State[SparkRecordsHeader, AdditiveUpdateResult[RecordSlot]] =
    add(SparkInternalHeader.addProjectedField(content))

  private def add[O](inner: State[SparkInternalHeader, O]): State[SparkRecordsHeader, O] =
    get[SparkRecordsHeader]
    .map(header => inner.run(header.internalHeader).value)
    .flatMap { case (newInternalHeader, value) => set(SparkRecordsHeader(newInternalHeader)).map(_ => value) }
}

final class SparkRecordHeaderOps(header: SparkRecordsHeader) {

  def +(content: SlotContent): SparkRecordsHeader =
    header.copy(header.internalHeader + content)

  def update[A](ops: State[SparkRecordsHeader, A]): (SparkRecordsHeader, A) =
    ops.run(header).value
}
