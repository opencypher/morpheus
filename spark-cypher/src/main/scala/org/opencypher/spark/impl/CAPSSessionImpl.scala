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
package org.opencypher.spark.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable

sealed class CAPSSessionImpl(val sparkSession: SparkSession) extends CAPSSession with Serializable {

  override type Result = RelationalCypherResult[DataFrameTable]

  override type Records = CAPSRecords

  override def cypherOnGraph(
    graph: PropertyGraph,
    query: String,
    queryParameters: CypherMap,
    maybeDrivingTable: Option[CypherRecords],
    queryCatalog: Map[QualifiedGraphName, PropertyGraph]
  ): Result = {
    val ambientGraphNew = mountAmbientGraph(graph)

    val maybeCapsRecords = maybeDrivingTable.map(_.asCaps)

    val inputFields = maybeCapsRecords match {
      case Some(inputRecords) => inputRecords.header.vars
      case None => Set.empty[Var]
    }

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query, inputFields)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR translation ...", newLine = false)

    val irBuilderContext = IRBuilderContext.initial(
      query,
      allParameters,
      semState,
      ambientGraphNew,
      qgnGenerator,
      catalog.listSources,
      catalog.view,
      inputFields,
      queryCatalog
    )
    val irOut = time("IR translation")(IRBuilder.process(stmt)(irBuilderContext))

    val ir = IRBuilder.extract(irOut)
    val queryLocalCatalog = IRBuilder.getContext(irOut).queryLocalCatalog

    logStageProgress("Done!")

    ir match {
      case cq: CypherQuery =>
        if (PrintIr.isSet) {
          println("IR:")
          println(cq.pretty)
        }
        planCypherQuery(graph, cq, allParameters, inputFields, maybeCapsRecords, queryLocalCatalog)

      case CreateGraphStatement(_, targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, maybeCapsRecords, queryLocalCatalog)
        val resultGraph = innerResult.graph
        catalog.store(targetGraph.qualifiedGraphName, resultGraph)
        RelationalCypherResult.empty

      case CreateViewStatement(_, qgn, parameterNames, queryString) =>
        catalog.store(qgn, parameterNames, queryString)
        RelationalCypherResult.empty

      case DeleteGraphStatement(_, qgn) =>
        catalog.dropGraph(qgn)
        RelationalCypherResult.empty

      case DeleteViewStatement(_, qgn) =>
        catalog.dropView(qgn)
        RelationalCypherResult.empty
    }
  }

  override def sql(query: String): CAPSRecords =
    records.wrap(sparkSession.sql(query))

  override def toString: String = s"${this.getClass.getSimpleName}"

}
