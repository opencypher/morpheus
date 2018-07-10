/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.{CypherSession, QualifiedGraphName}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.impl.QueryCatalog
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.operators.RelationalOperator

object RelationalPlannerContext {
  def from[T <: FlatRelationalTable[T]](
    session: CypherSession,
    catalog: QueryCatalog,
    inputRecords: RelationalCypherRecords[T],
    parameters: CypherMap
  ): RelationalPlannerContext[T] = RelationalPlannerContext(session, catalog, collection.mutable.Map.empty, inputRecords, parameters)
}

/**
  * Represents a back-end specific context which is used by the [[org.opencypher.okapi.relational.impl.physical.RelationalPlanner]].
  */
case class RelationalPlannerContext[T <: FlatRelationalTable[T]](
  /**
    * Refers to the session in which that query is executed.
    *
    * @return back-end specific cypher session
    */
  session: CypherSession,

  /**
    * Lookup function that resolves QGNs to property graphs.
    *
    * @return lookup function
    */
  catalog: QueryCatalog,

  // TODO: Improve design
  constructedGraphPlans: collection.mutable.Map[QualifiedGraphName, RelationalOperator[T]] = collection.mutable.Map.empty,

  /**
    * Initial records for physical planning.
    *
    * @return
    */
  inputRecords: RelationalCypherRecords[T],

  /**
    * Query parameters
    *
    * @return query parameters
    */
  parameters: CypherMap
)
