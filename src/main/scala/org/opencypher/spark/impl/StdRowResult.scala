package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark.{CypherResult, CypherFrame}

case class StdRowResult(frame: StdCypherFrame[Row])(implicit val context: StdFrameContext) extends CypherResult[Row] {

  override def toDS: Dataset[Row] = toDF

  override def toDF: DataFrame = {
    val flatFrame = frame.run
    val fieldSlots = frame.fieldSlots
    val columns = fieldSlots.map { case (field, slot) => new Column(slot.name.name).as(field.name) }
    flatFrame.select(columns: _*)
  }

  override def show(): Unit = {
    toDF.show(false)
  }
}
