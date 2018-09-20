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
package org.opencypher.spark.api

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.spark.api.io._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.graph.CAPSGraphFactory
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.impl.{CAPSRecords, CAPSRecordsFactory}

import scala.reflect.runtime.universe._

sealed class CAPSSession(val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with Serializable {

  protected implicit val caps: CAPSSession = this

  override lazy val catalog: CypherCatalog = new CypherCatalog

  override val records: CAPSRecordsFactory = CAPSRecordsFactory()

  override val graphs: CAPSGraphFactory = CAPSGraphFactory()

  /**
    * Reads a graph from sequences of nodes and relationships.
    *
    * @param nodes         sequence of nodes
    * @param relationships sequence of relationships
    * @tparam N node type implementing [[org.opencypher.spark.api.io.Node]]
    * @tparam R relationship type implementing [[org.opencypher.spark.api.io.Relationship]]
    * @return graph defined by the sequences
    */
  def readFrom[N <: Node : TypeTag, R <: Relationship : TypeTag](
    nodes: Seq[N],
    relationships: Seq[R] = Seq.empty
  ): PropertyGraph = {
    graphs.create(CAPSNodeTable(nodes), CAPSRelationshipTable(relationships))
  }

  /**
    * Reads a graph from a sequence of entity tables that contains at least one node table.
    *
    * @param nodeTable    first parameter to guarantee there is at least one node table
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*): PropertyGraph = {
    graphs.create(nodeTable, entityTables: _ *)
  }

  /**
    * Reads a graph from a sequence of entity tables that contains at least one node table.
    *
    * @param tags         tags that are used by graph entities
    * @param nodeTable    first parameter to guarantee there is at least one node table
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(tags: Set[Int], nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*): PropertyGraph = {
    graphs.create(tags, None, nodeTable, entityTables: _*)
  }

  // Store empty graph in catalog, so operators that start with an empty graph can refer to its QGN
  catalog.store(emptyGraphQgn, graphs.empty)

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

  def sql(query: String): CAPSRecords =
    records.wrap(sparkSession.sql(query))
}

object CAPSSession extends Serializable {

  /**
    * Creates a new [[org.opencypher.spark.api.CAPSSession]] based on the given [[org.apache.spark.sql.SparkSession]].
    *
    * @return CAPS session
    */
  def create(implicit sparkSession: SparkSession): CAPSSession = new CAPSSession(sparkSession)

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(settings: (String, String)*): CAPSSession = {
    val conf = new SparkConf(true)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.sql.shuffle.partitions", "12")
    conf.set("spark.default.parallelism", "8")
    conf.setAll(settings)

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

  /**
    * Import this into scope in order to use:
    *
    * {{{
    * import org.opencypher.caps.api.CAPSSession.RecordsAsDF
    * // ...
    * val df: DataFrame = results.records.asDF
    * }}}
    */
  implicit class RecordsAsDF(val records: CypherRecords) extends AnyVal {
    /**
      * Extracts the underlying [[org.apache.spark.sql#DataFrame]] from the given [[records]].
      *
      * Note that the column names in the returned DF do not necessarily correspond to the names of the Cypher RETURN
      * items, e.g. "RETURN n.name" does not mean that the column for that item is named "n.name".
      *
      * @return [[org.apache.spark.sql#DataFrame]] representing the records
      */
    def asDataFrame: DataFrame = records match {
      case caps: CAPSRecords => caps.table.df
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }

    /**
      * Converts all values stored in this table to instances of the corresponding CypherValue class.
      * In particular, this de-flattens, or collects, flattened entities (nodes and relationships) into
      * compact CypherNode/CypherRelationship objects.
      *
      * All values on each row are inserted into a CypherMap object mapped to the corresponding field name.
      *
      * @return [[org.apache.spark.sql.Dataset]] of CypherMaps
      */
    def asDataset: Dataset[CypherMap] = records match {
      case caps: CAPSRecords => caps.toCypherMaps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }
  }
}
