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
package org.opencypher.caps.api.graph

import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource.{Namespace => SessionNamespace}
import org.scalatest.{FunSuite, Matchers}

class QualifiedGraphNameTest extends FunSuite with Matchers {

  test("apply with string representation containing single namespace and single graph name") {
    val string = "testNamespace.testGraphName"
    QualifiedGraphName(string) should be(QualifiedGraphName(Namespace("testNamespace"), GraphName("testGraphName")))
  }

  test("apply with string representation containing single namespace and multiple graph name") {
    val string = "testNamespace.test.Graph.Name"
    QualifiedGraphName(string) should be(QualifiedGraphName(Namespace("testNamespace"), GraphName("test.Graph.Name")))
  }

  test("apply with string representation containing single grap name") {
    val string = "testGraphName"
    QualifiedGraphName(string) should be(QualifiedGraphName(SessionNamespace, GraphName("testGraphName")))
  }


}
