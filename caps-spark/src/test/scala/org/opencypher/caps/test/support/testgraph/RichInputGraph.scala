/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.test.support.testgraph

import scala.collection.immutable.Map

abstract class RichInputGraph[N, R](implicit n: N => RichInputNode, r: R => RichInputRelationship) {

  def allNodes: Set[N]

  def allRels: Set[R]
}

trait RichInputElement {

  def id: Long

  def properties: Map[String, AnyRef]

}

trait RichInputNode extends RichInputElement {

  def labels: Set[String]

}

trait RichInputRelationship extends RichInputElement {

  def relType: String

  def sourceId: Long

  def targetId: Long
}
