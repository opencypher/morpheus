/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.relational.api.planning

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}

/**
  * Responsible for tracking the context during the execution of a single query.
  *
  * @param sessionCatalog    mapping between graph names and graphs registered in the session catalog
  * @param maybeInputRecords optional driving table for the query
  * @param parameters        query parameters (e.g. constants) needed for expression evaluation
  * @param queryLocalCatalog mapping between graph names and graphs created during query execution
  * @param session           Cypher session
  * @tparam T Table type
  */
case class RelationalRuntimeContext[T <: Table[T]](
  sessionCatalog: QualifiedGraphName => Option[RelationalCypherGraph[T]],
  maybeInputRecords: Option[RelationalCypherRecords[T]] = None,
  parameters: CypherMap = CypherMap.empty,
  var queryLocalCatalog: Map[QualifiedGraphName, RelationalCypherGraph[T]] = Map.empty[QualifiedGraphName, RelationalCypherGraph[T]]
)(implicit val session: RelationalCypherSession[T]) {
  /**
    * Returns the graph referenced by the given QualifiedGraphName.
    *
    * @return back-end specific property graph
    */
  def resolveGraph(qgn: QualifiedGraphName): RelationalCypherGraph[T] = queryLocalCatalog.get(qgn) match {
    case None => sessionCatalog(qgn).getOrElse(throw IllegalArgumentException(s"a graph at $qgn"))
    case Some(g) => g
  }
}
