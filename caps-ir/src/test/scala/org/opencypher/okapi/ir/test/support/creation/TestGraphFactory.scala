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
package org.opencypher.okapi.test.support.creation

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.test.support.creation.propertygraph.{TestNode, TestPropertyGraph, TestRelationship}

trait TestGraphFactory[C <: CypherSession] {

  def apply(propertyGraph: TestPropertyGraph)(implicit caps: C): PropertyGraph

  def name: String

  override def toString: String = name

  def computeSchema(propertyGraph: TestPropertyGraph): Schema = {
    def extractFromNode(n: TestNode) =
      n.labels -> n.properties.value.map {
        case (name, prop) => name -> prop.cypherType
      }

    def extractFromRel(r: TestRelationship) =
      r.relType -> r.properties.value.map {
        case (name, prop) => name -> prop.cypherType
      }

    val labelsAndProps = propertyGraph.nodes.map(extractFromNode)
    val typesAndProps = propertyGraph.relationships.map(extractFromRel)

    val schemaWithLabels = labelsAndProps.foldLeft(Schema.empty) {
      case (acc, (labels, props)) => acc.withNodePropertyKeys(labels, props)
    }

    typesAndProps.foldLeft(schemaWithLabels) {
      case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
    }
  }
}
