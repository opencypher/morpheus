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
package org.opencypher.okapi.api.graph

import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

class CypherSessionTest extends FunSpec with MockitoSugar with Matchers {

  it("avoid de-registering the session data source") {
    an[org.opencypher.okapi.impl.exception.UnsupportedOperationException] should be thrownBy
      createSession.deregisterSource(SessionGraphDataSource.Namespace)
  }

  it("avoid de-registering a non-registered data source") {
    an[IllegalArgumentException] should be thrownBy createSession.deregisterSource(Namespace("foo"))
  }

  it("avoids registering a data source with an existing namespace") {
    val session = createSession
    val namespace = Namespace("foo")
    session.registerSource(namespace, mock[PropertyGraphDataSource])
    an[IllegalArgumentException] should be thrownBy session.registerSource(namespace, mock[PropertyGraphDataSource])
  }

  it("register data source") {
    val session = createSession
    val namespace = Namespace("foo")
    val dataSource = mock[PropertyGraphDataSource]
    session.registerSource(namespace, dataSource)
    session.catalog.source(namespace) should equal(dataSource)
  }

  it("de-register data source") {
    val session = createSession
    val namespace = Namespace("foo")
    val dataSource = mock[PropertyGraphDataSource]
    session.registerSource(namespace, dataSource)
    session.catalog.source(namespace) should equal(dataSource)
    session.deregisterSource(namespace)
    an[IllegalArgumentException] should be thrownBy session.catalog.source(namespace)
    an[IllegalArgumentException] should be thrownBy session.catalog.source(namespace)
  }

  it("namespaces") {
    val session = createSession
    session.catalog.namespaces should equal(Set(SessionGraphDataSource.Namespace))
    val namespace = Namespace("foo")
    val dataSource = mock[PropertyGraphDataSource]
    session.registerSource(namespace, dataSource)
    session.catalog.namespaces should equal(Set(SessionGraphDataSource.Namespace, namespace))
  }

  private def createSession: CypherSession = new CypherSession {
    override val catalog: PropertyGraphCatalog = new CypherCatalog()

    override def cypher(
      query: String,
      parameters: CypherMap,
      drivingTable: Option[CypherRecords],
      queryCatalog: Map[QualifiedGraphName, PropertyGraph]): Result = ???

    override private[opencypher] def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, drivingTable: Option[CypherRecords], queryCatalog: Map[QualifiedGraphName, PropertyGraph]) = ???

    override def generateQualifiedGraphName: QualifiedGraphName = ???
  }
}
