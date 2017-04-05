package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc
import org.opencypher.spark_legacy.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark_legacy.impl.{ProductFrame, StdCypherFrame, StdRuntimeContext, StdSlot}

sealed trait SortOrder
case object Asc extends SortOrder
case object Desc extends SortOrder

final case class SortItem(key: Symbol, order: SortOrder)
object OrderBy extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(item: SortItem): StdCypherFrame[Product] = {
    val keySlot = obtain(input.signature.slot)(item.key)
    OrderBy(input)(keySlot -> item.order)
  }

  private final case class OrderBy(input: StdCypherFrame[Product])(slotItem: (StdSlot, SortOrder))
    extends ProductFrame(input.signature) {

    override protected def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val (slot, order) = slotItem

      val out = slot.representation match {
        case EmbeddedRepresentation(_) =>
          order match {
            case Asc => in.sort(in.columns(slot.ordinal))
            case Desc =>
//              context.session.createDataset(in.rdd.sortBy(_.productElement(slot.ordinal).asInstanceOf[Long], ascending = false))(context.productEncoder(slots))
              in.sort(desc(in.columns(slot.ordinal)))
          }

        case BinaryRepresentation =>
          throw new UnsupportedOperationException("Need to implement orderability for complex values")
//          val ordering = order match {
//            case Asc => CypherValue.orderability
//            case Desc => CypherValue.reverseOrderability
//          }
//          val sortedRdd = in.rdd.sortBy(OrderByColumn(slot.ordinal))
//          context.session.createDataset(sortedRdd)(context.productEncoder(slots))
      }

      out
    }
  }

//  private final case class OrderByColumn(index: Int) extends (Product => CypherValue) {
//
//    import org.opencypher.spark.impl.util._
//
//    override def apply(product: Product): CypherValue = {
//      product.getAs[CypherValue](index)
//    }
//  }

}
