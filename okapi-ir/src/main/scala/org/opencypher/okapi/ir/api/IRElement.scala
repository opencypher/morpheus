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
package org.opencypher.okapi.ir.api

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.set.SetItem

object IRField {
  def relTypes(field: IRField): Set[String] = field.cypherType match {
    case CTRelationship(types, _) => types
    case _ => Set.empty
  }
}

final case class IRField(name: String)(val cypherType: CypherType = CTWildcard) {
  override def toString = s"$name :: $cypherType"

  def escapedName: String = name.replaceAll("`", "``")

  def toTypedTuple: (String, CypherType) = name -> cypherType
}

sealed trait IRGraph {
  def schema: Schema

  def qualifiedGraphName: QualifiedGraphName
}

object IRCatalogGraph {
  def apply(name: String, schema: Schema): IRCatalogGraph =
    IRCatalogGraph(QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName(name)), schema)
}

final case class IRCatalogGraph(qualifiedGraphName: QualifiedGraphName, schema: Schema) extends IRGraph

final case class IRInstantiatedView(
  qualifiedGraphName: QualifiedGraphName,
  graph: PropertyGraph,
  description: String
) extends IRGraph {
  override def schema: Schema = graph.schema
}

final case class IRPatternGraph(
  qualifiedGraphName: QualifiedGraphName,
  schema: Schema,
  clones: Map[IRField, Expr],
  creates: Pattern,
  sets: List[SetItem],
  onGraphs: List[QualifiedGraphName]
) extends IRGraph
