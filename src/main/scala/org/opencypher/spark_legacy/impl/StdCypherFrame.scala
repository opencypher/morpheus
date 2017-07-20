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
package org.opencypher.spark_legacy.impl

import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.opencypher.spark_legacy.api.frame.{CypherSlot, OldCypherFrame, Representation}
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}

abstract class StdCypherFrame[Out](sig: StdFrameSignature)
  extends OldCypherFrame[Out] {

  override type Frame = StdCypherFrame[Out]
  override type RuntimeContext = StdRuntimeContext
  override type Field = StdField
  override type Slot = StdSlot
  override type Signature = StdFrameSignature

  // we need these overrides to work around a highlighting bug in the intellij scala plugin

  override def signature: StdFrameSignature = sig
  override def fields: Seq[StdField] = signature.fields
  override def slots: Seq[StdSlot] = signature.slots

  override def run(implicit context: RuntimeContext): Dataset[Out] =
    execute

  protected def execute(implicit context: RuntimeContext): Dataset[Out]

  // Spark renames columns when we convert between Dataset types.
  // To keep track of them we rename them explicitly after each step.
  protected def alias[T](input: Dataset[T])(implicit encoder: Encoder[T]): Dataset[T] =
    input.toDF(signature.slotNames: _*).as(encoder)
}

case class StdSlot(sym: Symbol, cypherType: CypherType, ordinal: Int, representation: Representation) extends CypherSlot

abstract class ProductFrame(sig: StdFrameSignature) extends StdCypherFrame[Product](sig) {

  override def run(implicit context: RuntimeContext): Dataset[Product] = execute
//    alias(execute)(context.productEncoder(slots))
}

abstract class RowFrame(sig: StdFrameSignature) extends StdCypherFrame[Row](sig) {
  override def run(implicit context: RuntimeContext): Dataset[Row] =
    execute.toDF(signature.slotNames: _*)
}

abstract class NodeFrame(sig: StdFrameSignature) extends StdCypherFrame[CypherNode](sig) {
  override def run(implicit context: StdRuntimeContext): Dataset[CypherNode] = execute
//    alias(execute)(context.cypherNodeEncoder)
}

abstract class RelationshipFrame(sig: StdFrameSignature) extends StdCypherFrame[CypherRelationship](sig) {
  override def run(implicit context: StdRuntimeContext): Dataset[CypherRelationship] =
    alias(execute)(context.cypherRelationshipEncoder)
}
