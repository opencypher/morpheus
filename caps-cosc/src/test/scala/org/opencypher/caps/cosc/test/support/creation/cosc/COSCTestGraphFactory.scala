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
package org.opencypher.caps.cosc.test.support.creation.cosc

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.cosc.impl.value.{COSCNode, COSCRelationship}
import org.opencypher.caps.cosc.impl.{COSCGraph, COSCSession}
import org.opencypher.caps.test.support.creation.TestGraphFactory
import org.opencypher.caps.test.support.creation.propertygraph.TestPropertyGraph

object COSCTestGraphFactory extends TestGraphFactory[COSCSession] {

  override def apply(propertyGraph: TestPropertyGraph)(implicit caps: COSCSession): PropertyGraph = {
    val nodes = propertyGraph.nodes.map(n => COSCNode(n.id, n.labels, n.properties))
    val rels = propertyGraph.relationships.map(r => COSCRelationship(r.id, r.source, r.target, r.relType, r.properties))

    COSCGraph.create(nodes, rels)
  }

  override def name: String = "COSCTestGraphFactory"
}
