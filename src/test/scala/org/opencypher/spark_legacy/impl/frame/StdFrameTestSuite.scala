/**
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
package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark_legacy.api.frame.{CypherFrameSignature, Representation}
import org.opencypher.spark_legacy.impl._
import org.opencypher.spark_legacy.impl.frame.StdFrameTestSuite.FrameTestResult
import org.opencypher.spark_legacy.impl.util.SlotSymbolGenerator
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value.{NodeData, RelationshipData}
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}
import org.opencypher.spark_legacy.PropertyGraphFactory

object StdFrameTestSuite {
  final case class FrameTestResult[Out](dataframe: Dataset[Out], signature: StdFrameSignature) {
    def toList = dataframe.collect().toList
    def toSet = dataframe.collect().toSet
  }
}

abstract class StdFrameTestSuite extends BaseTestSuite with SparkTestSession {

  implicit val factory = PropertyGraphFactory.create

  override protected def beforeEach(): Unit = {
    factory.clear()
  }

  trait GraphTest {
    val graph = factory.graph

    implicit val planningContext =
      new PlanningContext(new SlotSymbolGenerator, graph.nodes, graph.relationships)

    val frames = new FrameProducer

    implicit val runtimeContext = new StdRuntimeContext(session, Map.empty)
  }

  def add(nodeData: NodeData) = factory.add(nodeData)
  def add(relationshipData: RelationshipData) = factory.add(relationshipData)

  implicit final class RichTestFrame[Out](val frame: StdCypherFrame[Out]) extends AnyRef {

    def testResult(implicit context: StdRuntimeContext) = {
      val out = frame.run(context)
//      out.columns should equal(frame.slots.map(_.sym.name))
      FrameTestResult(out, frame.signature)
    }
  }

  implicit final class RichFrameSignature(val sig: CypherFrameSignature) extends AnyRef {

    def shouldHaveFields(expected: (Symbol, CypherType)*): Unit = {
      sig.fields.map { field => field.sym -> field.cypherType } should equal(expected)
    }

    def shouldHaveFieldSlots(expected: (Symbol, Representation)*): Unit = {
      sig.fields.flatMap { f => sig.slot(f.sym).map(s => f.sym -> s.representation) }.toSet should equal(expected.toSet)
    }
  }
}
