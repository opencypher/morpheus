package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark._

abstract class StdCypherFrame(val fields: Seq[StdField], val slots: Map[Symbol, StdSlot]) extends CypherFrame {
  override type Frame = StdCypherFrame
  override type Field = StdField
  override type Slot = StdSlot

  // Build actual result
  override def result: CypherResult = new CypherResult {

    override def toDF: DataFrame = {
      val data = execute

      val columns = fields.map { field =>
        val slot = slots(field.name)
        new Column(slot.name.name).as(field.name)
      }
      val result = data.select(columns: _*)
      result.explain(true)
      result
    }

    override def toDS[T: Encoder](f: (CypherRecord) => T): Dataset[T] = ???

    override def show(): Unit = {
      toDF.show(false)
    }
  }

  protected def execute: DataFrame

//  private def slot(field: Field): Slot = slots(field.name)
}


case class StdField(name: Symbol, cypherType: CypherType) extends CypherField with Serializable

case class StdSlot(name: Symbol, cypherType: CypherType, representation: Representation) extends CypherSlot with Serializable
