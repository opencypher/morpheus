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
package org.opencypher.spark_legacy.api.frame

import org.apache.spark.sql.Dataset

// A CypherFrame is a frame of cypher records. It doubles as
//
// - plan operator for cypher on spark
// - knows how to produce a concrete cypher result
//
trait OldCypherFrame[Out] {

  // Implementations may specialize
  //
  type Frame <: OldCypherFrame[Out]
  type Field <: CypherField
  type Slot <: CypherSlot
  type RuntimeContext <: CypherRuntimeContext
  type Signature <: CypherFrameSignature

  // This is a two layer construct

  def signature: Signature

  //
  // On the top level, there is the signature of the covered cypher record fields
  //
  def fields: Seq[Field]

  //
  // On the bottom level and somewhat internally, the frame tracks slots that hold
  // the results of evaluating certain expressions
  //
  def slots: Seq[Slot]

  // Expressions are not only evaluated over slots but in a wider context
  //  def parameters: Map[Symbol, CypherValue]

  def run(implicit context: RuntimeContext): Dataset[Out]
}
