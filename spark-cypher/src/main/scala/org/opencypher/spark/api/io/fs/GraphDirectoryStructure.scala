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
package org.opencypher.spark.api.io.fs

import org.opencypher.okapi.api.graph.GraphName

trait GraphDirectoryStructure {

  def dataSourceRootPath: String

  def pathToGraphDirectory(graphName: GraphName): String

  def pathToGraphSchema(graphName: GraphName): String

  def pathToCAPSMetaData(graphName: GraphName): String

  def pathToNodeTable(graphName: GraphName, labels: Set[String]): String

  def pathToRelationshipTable(graphName: GraphName, relKey: String): String

}

object DefaultGraphDirectoryStructure {

  implicit class StringPath(val path: String) extends AnyVal {
    def /(segment: String): String = s"$path$pathSeparator$segment"
  }

  implicit class GraphPath(graphName: GraphName) {
    def path: String = graphName.value.replace(".", pathSeparator)
  }

  val pathSeparator = "/"

}

case class DefaultGraphDirectoryStructure(dataSourceRootPath: String) extends GraphDirectoryStructure {

  import DefaultGraphDirectoryStructure._

  override def pathToGraphDirectory(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path
  }

  override def pathToGraphSchema(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path / "schema.json"
  }

  override def pathToCAPSMetaData(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path / "capsGraphMetaData.json"
  }

  override def pathToNodeTable(graphName: GraphName, labels: Set[String]): String = {
    dataSourceRootPath / graphName.path / "nodes" / labels.toSeq.sorted.mkString("_")
  }

  override def pathToRelationshipTable(graphName: GraphName, relKey: String): String = {
    dataSourceRootPath / graphName.path / "relationships" / relKey
  }

}
