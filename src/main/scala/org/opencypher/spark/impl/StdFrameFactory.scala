package org.opencypher.spark.impl

import org.apache.spark.sql.{Row, DataFrame, Dataset}
import org.opencypher.spark.{CypherType, CypherValue, BinaryRepresentation, CypherNode}
import org.opencypher.spark.CypherTypes.CTNode

class StdFrameFactory(slotNames: SlotNameGenerator) {

  def productFrame(input: StdCypherFrame[Row]): StdCypherFrame[Product] = {
    new StdCypherFrame[Product](input.fields, input.slots) {
      override def run(implicit context: FrameContext): Dataset[Product] = {
        input.run.as[Product](productEncoder)
      }
    }
  }

  def propertyAccess(input: StdCypherFrame[Product], node: Symbol, propertyKey: Symbol)(output: StdField): StdCypherFrame[Product] = {
    val newFields = input.fields :+ output
    val newSlots = input.slots :+ StdSlot(slotNames.newSlotName(output.name), output.cypherType, BinaryRepresentation)
    val (field, index) = input.fields.zipWithIndex.find {
      case (f, _) => f.name == node
    }.get

    new StdCypherFrame[Product](newFields, newSlots) {
      override def run(implicit context: FrameContext): Dataset[Product] = {
        val inputFrame = input.run
        inputFrame.map({ product =>
          val elts = product.productIterator.toVector
          val node = elts(index).asInstanceOf[CypherNode]
          val value = node.properties.get(propertyKey.name).orNull
          productize(elts :+ value)
        })(productEncoder)
      }
    }
  }


  def nodeFrame(input: Dataset[CypherNode])(name: Symbol): StdCypherFrame[CypherNode] = {
    new StdCypherFrame[CypherNode](
      fields = Seq(StdField(name, CTNode)),
      slots = Seq(StdSlot(slotNames.newSlotName(name), CTNode, BinaryRepresentation))
    ) {
      override def run(implicit context: FrameContext): Dataset[CypherNode] = input
    }
  }

  def rowFrame[T <: CypherValue](input: StdCypherFrame[T]): StdCypherFrame[Row] = {
    new StdCypherFrame[Row](input.fields, input.slots) {
      override def run(implicit context: FrameContext): Dataset[Row] = {
        input.run.toDF(slots.head.name.name)
      }
    }
  }

}

