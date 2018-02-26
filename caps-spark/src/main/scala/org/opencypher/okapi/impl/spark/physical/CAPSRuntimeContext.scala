/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.okapi.impl.spark.physical

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.spark.physical.operators.CAPSPhysicalOperator
import org.opencypher.okapi.impl.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.okapi.relational.api.physical.RuntimeContext

import scala.collection.mutable

object CAPSRuntimeContext {
  val empty = CAPSRuntimeContext(CypherMap.empty, _ => None, mutable.Map.empty)
}

case class CAPSRuntimeContext(
  parameters: CypherMap,
  resolve: QualifiedGraphName => Option[CAPSGraph],
  cache: mutable.Map[CAPSPhysicalOperator, CAPSPhysicalResult])
  extends RuntimeContext[CAPSRecords, CAPSGraph]


