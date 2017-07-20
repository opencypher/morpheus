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
import org.opencypher.spark.api.types.{CTAny, CTList}
import org.opencypher.spark.api.value.CypherList
import org.opencypher.spark_legacy.impl._

import scala.collection.TraversableOnce

object Unwind extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(list: Symbol, item: Symbol)(implicit context: PlanningContext): StdCypherFrame[Product] = {

    val listInnerType = input.signature.field(list).get.cypherType match {
      case CTList(inner) => inner
      case t if t.superTypeOf(CTList(CTAny)).isTrue => CTAny.nullable
      case x => throw new IllegalArgumentException(s"Expected $list to be a list, but it was a $x")
    }
    val (_, outSig) = input.signature.addField(item -> listInnerType)
    val listSlotOrdinal = input.signature.slot(list).get.ordinal

    Unwind(input)(listSlotOrdinal)(outSig)
  }

  private final case class Unwind(input: StdCypherFrame[Product])(index: Int)(sig: StdFrameSignature) extends ProductFrame(sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      val out = in.flatMap(addItemsFromListAt(index))(context.productEncoder(sig.slots))

      out
    }

  }

  private final case class addItemsFromListAt(index: Int) extends (Product => TraversableOnce[Product]) {
    import org.opencypher.spark_legacy.impl.util._

    override def apply(record: Product): TraversableOnce[Product] = {
      val list = record.getAs[CypherList](index)
      if (list == null) IndexedSeq.empty
      else list.mapToTraversable { item =>
        record :+ item
      }
    }
  }
}
