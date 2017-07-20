/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
