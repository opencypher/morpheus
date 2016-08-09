package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark.{CypherResult, CypherFrame}

case class StdRowResult(frame: StdCypherFrame[Row])(implicit val context: StdRuntimeContext) extends CypherResult[Row] {

  override def toDS: Dataset[Row] = toDF

  override def toDF: DataFrame = {
    val flatRows = frame.run
    val fieldSlots = frame.fieldSlots
    val columns = fieldSlots.map { case (field, slot) => new Column(slot.sym.name).as(field.sym) }
    flatRows.select(columns: _*)
  }

  override def show(): Unit = {
    toDF.show(false)
  }
}
