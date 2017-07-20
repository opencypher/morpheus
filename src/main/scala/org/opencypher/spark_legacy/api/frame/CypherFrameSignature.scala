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

import org.opencypher.spark_legacy.api._
import org.opencypher.spark_legacy.impl.PlanningContext

trait CypherFrameSignature {

  type Field <: CypherField
  type Slot <: CypherSlot

  def field(sym: Symbol): Option[Field]
  def slot(name: Symbol): Option[Slot]

  def addField(symbol: TypedSymbol)(implicit context: PlanningContext): (Field, CypherFrameSignature)
  def addFields(symbols: TypedSymbol*)(implicit console: PlanningContext): (Seq[Field], CypherFrameSignature)

  def upcastField(symbol: TypedSymbol): (Field, CypherFrameSignature)
  def aliasField(alias: Alias): (Field, CypherFrameSignature)
  def selectFields(fields: Symbol*): (Seq[Slot], CypherFrameSignature)

  def dropField(symbol: Symbol): CypherFrameSignature

  def slots: Seq[Slot]
  def fields: Seq[Field]
}
