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
package org.opencypher.okapi.ir.api

import org.opencypher.okapi.api.graph.{GraphName, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.set.SetItem

object IRField {
  def relTypes(field: IRField): Set[String] = field.cypherType match {
    case CTRelationship(types) => types
    case _                     => Set.empty
  }
}

final case class IRField(name: String)(val cypherType: CypherType = CTWildcard) {
  override def toString = s"$name :: $cypherType"

  def escapedName: String = name.replaceAll("`", "``")

  def toTypedTuple: (String, CypherType) = name -> cypherType
}

// TODO: IRGraph[Expr]
sealed trait IRGraph {
  def schema: Schema
}

object IRCatalogGraph {
  def apply(name: String, schema: Schema): IRCatalogGraph =
    IRCatalogGraph(QualifiedGraphName(SessionPropertyGraphDataSource.Namespace, GraphName(name)), schema)
}

final case class IRCatalogGraph(qualifiedName: QualifiedGraphName, schema: Schema) extends IRGraph

final case class IRPatternGraph[E](schema: Schema, creates: Pattern[E], sets: List[SetItem[E]]) extends IRGraph
