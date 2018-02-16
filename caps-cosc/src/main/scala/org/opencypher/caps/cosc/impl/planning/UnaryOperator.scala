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
package org.opencypher.caps.cosc.impl.planning

import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.cosc.impl.{COSCPhysicalResult, COSCRecords, COSCRuntimeContext}
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.table.{ColumnName, RecordHeader}
import org.opencypher.caps.ir.api.expr.{Expr, Var}
import org.opencypher.caps.logical.impl.{LogicalExternalGraph, LogicalGraph}

abstract class UnaryOperator extends COSCOperator {

  def in: COSCOperator

  override def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult = executeUnary(in.execute)

  def executeUnary(prev: COSCPhysicalResult)(implicit context: COSCRuntimeContext): COSCPhysicalResult
}

case class SetSourceGraph(in: COSCOperator, graph: LogicalExternalGraph) extends UnaryOperator with InheritedHeader {

  override def executeUnary(prev: COSCPhysicalResult)(implicit context: COSCRuntimeContext): COSCPhysicalResult =
    prev.withGraph(graph.name -> resolve(graph.qualifiedGraphName))
}

case class Scan(in: COSCOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: COSCPhysicalResult)(implicit context: COSCRuntimeContext): COSCPhysicalResult = {
    val graphs = prev.graphs
    val graph = graphs(inGraph.name)
    val records = v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        throw IllegalArgumentException("an entity type", x)
    }
    assert(header == records.header)
    COSCPhysicalResult(records, graphs)
  }

}

case class Select(in: COSCOperator, fields: Seq[Var], graphs: Set[String], header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: COSCPhysicalResult)(implicit context: COSCRuntimeContext): COSCPhysicalResult = prev
}

case class Project(in: COSCOperator, expr: Expr, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: COSCPhysicalResult)(implicit context: COSCRuntimeContext): COSCPhysicalResult = {
    val headerNames = header.slotsFor(expr).map(ColumnName.of)
    val dataNames = prev.records.data.columns.toSeq
    val data = prev.records.data

    val newData = headerNames.diff(dataNames) match {
      case Seq(one) =>
        println(s"Projecting $expr to key $one")
        data.project(expr, one)(header, context)
    }

    COSCPhysicalResult(COSCRecords.create(newData, header), prev.graphs)
  }
}

case class Filter(in: COSCOperator, expr: Expr, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: COSCPhysicalResult)(implicit context: COSCRuntimeContext): COSCPhysicalResult = {
    println(s"Filtering based on predicate: $expr")
    val newData = prev.records.data.filter(expr)(header, context)
    COSCPhysicalResult(COSCRecords.create(newData, header), prev.graphs)
  }
}
