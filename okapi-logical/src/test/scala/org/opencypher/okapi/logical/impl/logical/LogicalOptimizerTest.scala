/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.logical.impl.logical

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, _}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, _}
import org.opencypher.okapi.ir.impl.QueryLocalCatalog
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.logical.impl
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper._
import org.opencypher.okapi.testing.support.TreeVerificationSupport
import org.opencypher.okapi.trees.BottomUp

import scala.collection.Map
import scala.language.implicitConversions

class LogicalOptimizerTest extends BaseTestSuite with IrConstruction with LogicalConstruction with TreeVerificationSupport {

  val emptySqm: SolvedQueryModel = SolvedQueryModel.empty
  val logicalGraph = LogicalCatalogGraph(testQualifiedGraphName, Schema.empty)
  val emptySchema: Schema = Schema.empty
  val emptyGraph = TestGraph(emptySchema)

  //  //Helper to create nicer expected results with `asCode`
  //  import org.opencypher.caps.impl.common.AsCode._
  //  implicit val specialMappings = Map[Any, String](
  //    schema -> "schema",
  //    emptySqm -> "emptySqm",
  //    logicalGraph -> "logicalGraph",
  //    emptySqm -> "emptySqm",
  //    (CTNode: CTNode) -> "CTNode"
  //  )

  it("rewrites missing label scan to empty records") {
    val query =
      """|MATCH (a:Animal)
         |RETURN a""".stripMargin
    val plan = logicalPlan(query, emptyGraph)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(emptyGraph))

    val aVar = Var("a")(CTNode(Set("Animal")))

    val emptyRecords = EmptyRecords(
      Set(aVar),
      Start(logicalGraph, emptySqm),
      SolvedQueryModel(Set(IRField("a")(CTNode(Set("Animal")))), Set(HasLabel(aVar, Label("Animal"))(CTBoolean)))
    )

    val filter = Filter(
      IsNotNull(aVar)(CTBoolean),
      emptyRecords,
      SolvedQueryModel(Set(IRField("a")(CTNode(Set("Animal")))), Set(HasLabel(aVar, Label("Animal"))(CTBoolean), IsNotNull(aVar)(CTBoolean)))
    )

    val expected = Select(
      List(aVar),
      filter,
      SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean), IsNotNull(aVar)(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  it("rewrites missing label combination") {
    val query =
      """|MATCH (a:Animal:Astronaut)
         |RETURN a""".stripMargin
    val schema = Schema.empty.withNodePropertyKeys("Animal")().withNodePropertyKeys("Astronaut")()
    val logicalGraph = LogicalCatalogGraph(testQualifiedGraphName, schema)

    val plan = logicalPlan(query, TestGraph(schema))
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(TestGraph(schema)))

    val aVar = Var("a")(CTNode(Set("Astronaut", "Animal")))
    val emptyRecords = EmptyRecords(
      Set(aVar),
      Start(logicalGraph, emptySqm),
      SolvedQueryModel(
        Set(IRField("a")(CTNode(Set("Astronaut", "Animal")))),
        Set(
          HasLabel(aVar, Label("Astronaut"))(CTBoolean),
          HasLabel(aVar, Label("Animal"))(CTBoolean)
        )
      )
    )

    val filter = Filter(
      IsNotNull(aVar)(CTBoolean),
      emptyRecords,
      SolvedQueryModel(
        Set(IRField("a")(CTNode(Set("Astronaut", "Animal")))),
        Set(
          IsNotNull(aVar)(CTBoolean),
          HasLabel(aVar, Label("Astronaut"))(CTBoolean),
          HasLabel(aVar, Label("Animal"))(CTBoolean)
        )
      )
    )

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal", "Astronaut")))),
      filter,
      SolvedQueryModel(
        Set(IRField("a")(CTNode)),
        Set(
          IsNotNull(aVar)(CTBoolean),
          HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean),
          HasLabel(Var("a")(CTNode), Label("Astronaut"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  describe("replace cartesian with ValueJoin") {

    it("should replace cross with value join if filter is present") {
      val startA = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val startB = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val varA = Var("a")(CTNode)
      val propA = expr.Property(varA, PropertyKey("name"))(CTString)
      val varB = Var("b")(CTNode)
      val propB = expr.Property(varB, PropertyKey("name"))(CTString)
      val equals = Equals(propA, propB)(CTBoolean)
      val irFieldA = IRField(varA.name)(varA.cypherType)
      val irFieldB = IRField(varB.name)(varB.cypherType)

      val scanA = planFilters(PatternScan.nodeScan(varA, startA, SolvedQueryModel(Set(irFieldA))), varA)
      val scanB = planFilters(PatternScan.nodeScan(varB, startB, SolvedQueryModel(Set(irFieldB))), varB)
      val cartesian = CartesianProduct(scanA, scanB, scanA.solved ++ scanB.solved)
      val filter = Filter(equals, cartesian, cartesian.solved)

      val optimizedPlan = BottomUp[LogicalOperator](LogicalOptimizer.replaceCartesianWithValueJoin).transform(filter)

      val projectA = Project(propA -> None, scanA, scanA.solved)
      val projectB = Project(propB -> None, scanB, scanB.solved)
      val solved = (projectA.solved ++ projectB.solved).withPredicate(equals)
      val valueJoin = ValueJoin(projectA, projectB, Set(equals), solved)

      optimizedPlan should equalWithTracing(valueJoin)
    }

    it("should replace cross with value join if filter with flipped predicate is present") {
      val startA = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val startB = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val varA = Var("a")(CTNode)
      val propA = expr.Property(varA, PropertyKey("name"))(CTString)
      val varB = Var("b")(CTNode)
      val propB = expr.Property(varB, PropertyKey("name"))(CTString)
      val equals = Equals(propB, propA)(CTBoolean)
      val irFieldA = IRField(varA.name)(varA.cypherType)
      val irFieldB = IRField(varB.name)(varB.cypherType)

      val scanA = PatternScan.nodeScan(varA, startA, SolvedQueryModel(Set(irFieldA)))
      val scanB = PatternScan.nodeScan(varB, startB, SolvedQueryModel(Set(irFieldB)))
      val cartesian = CartesianProduct(scanA, scanB, SolvedQueryModel(Set(irFieldA, irFieldB)))
      val filter = Filter(equals, cartesian, SolvedQueryModel(Set(irFieldA, irFieldB)))

      val optimizedPlan = BottomUp[LogicalOperator](LogicalOptimizer.replaceCartesianWithValueJoin).transform(filter)

      val flippedEquals = Equals(propA, propB)(CTBoolean)
      val projectA = Project(propA -> None, scanA, scanA.solved)
      val projectB = Project(propB -> None, scanB, scanB.solved)
      val solved = SolvedQueryModel(Set(irFieldA, irFieldB)).withPredicate(flippedEquals)
      val valueJoin = ValueJoin(projectA, projectB, Set(flippedEquals), solved)

      optimizedPlan should equalWithTracing(valueJoin)
    }

    it("should replace cross with value join for driving tables") {
      val nameField = 'name -> CTString
      val startDrivingTable = impl.DrivingTable(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), Set(nameField), SolvedQueryModel.empty.withField(nameField))

      val startB = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val varB = Var("b")(CTNode)
      val propB = expr.Property(varB, PropertyKey("name"))(CTString)

      val equals = Equals(nameField, propB)(CTBoolean)
      val irFieldB = IRField(varB.name)(varB.cypherType)

      val scanB = PatternScan.nodeScan(varB, startB, SolvedQueryModel(Set(irFieldB)))
      val cartesian = CartesianProduct(startDrivingTable, scanB, SolvedQueryModel(Set(nameField, irFieldB)))
      val filter = Filter(equals, cartesian, SolvedQueryModel(Set(nameField, irFieldB)))

      val optimizedPlan = BottomUp[LogicalOperator](LogicalOptimizer.replaceCartesianWithValueJoin).transform(filter)

      val projectName = Project(toVar(nameField) -> None, startDrivingTable, startDrivingTable.solved)
      val projectB = Project(propB -> None, scanB, scanB.solved)

      val solved = SolvedQueryModel(Set(nameField, irFieldB)).withPredicate(equals)
      val valueJoin = ValueJoin(projectName, projectB, Set(equals), solved)

      optimizedPlan should equalWithTracing(valueJoin)
    }
  }

  describe("insert pattern scans") {
    val schema = Schema.empty
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("C")()
      .withRelationshipPropertyKeys("B")()

    it("inserts NodeRelPatterns") {
      val pattern = NodeRelPattern(CTNode(Set("A"), Some(testQualifiedGraphName)), CTRelationship(Set("B"), Some(testQualifiedGraphName)))
      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (a:A)-[b:B]->(c:C) RETURN a, b, c""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))

      optimizedPlan.occourences[ValueJoin] should be(1)
      optimizedPlan.occourences[PatternScan] should be(2)

      optimizedPlan.exists {
        case _: Expand => true
        case _ => false
      } should be(false)

      optimizedPlan.exists {
        case PatternScan(otherPattern, map, _, _) =>
          pattern == otherPattern &&
          map == Map(Var("a")(CTNode) -> pattern.nodeEntity, Var("b")(CTRelationship) -> pattern.relEntity)
        case _ => false
      } should be(true)
    }

    it("inserts connecting NodeRelPatterns") {
      val pattern = NodeRelPattern(CTNode(Set("A"), Some(testQualifiedGraphName)), CTRelationship(Set("B"), Some(testQualifiedGraphName)))
      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (c1:C)<-[b1:B]-(a:A)-[b2:B]->(c2:C) RETURN a""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))

      optimizedPlan.occourences[ValueJoin] should be(3)
      optimizedPlan.occourences[PatternScan] should be(4)
      optimizedPlan.exists {
        case _: Expand => true
        case _ => false
      } should be(false)

      optimizedPlan.exists {
        case PatternScan(otherPattern, map, _, _) =>
          pattern == otherPattern &&
            map == Map(Var("a")(CTNode) -> pattern.nodeEntity, Var("b1")(CTRelationship) -> pattern.relEntity)
        case _ => false
      } should be(true)

      optimizedPlan.exists {
        case PatternScan(otherPattern, map, _, _) =>
          pattern == otherPattern &&
            map == Map(Var("a")(CTNode) -> pattern.nodeEntity, Var("b2")(CTRelationship) -> pattern.relEntity)
        case _ => false
      } should be(true)
    }

    it("inserts TripletPatterns") {
      val pattern = TripletPattern(
        CTNode(Set("A"), Some(testQualifiedGraphName)),
        CTRelationship(Set("B"), Some(testQualifiedGraphName)),
        CTNode(Set("C"), Some(testQualifiedGraphName))
      )

      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (a:A)-[b:B]->(c:C) RETURN a, b, c""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))

      optimizedPlan.occourences[ValueJoin] should be(0)
      optimizedPlan.occourences[PatternScan] should be(1)

      optimizedPlan.exists {
        case _: Expand => true
        case _ => false
      } should be(false)

      optimizedPlan.exists {
        case PatternScan(otherPattern, map, _, _) =>
          pattern == otherPattern &&
            map == Map(Var("a")(CTNode) -> pattern.sourceEntity, Var("b")(CTRelationship) -> pattern.relEntity, Var("c")(CTNode) -> pattern.targetEntity)
        case _ => false
      } should be(true)
    }

    it("inserts connecting TripletPatterns") {
      val pattern = TripletPattern(
        CTNode(Set("A"), Some(testQualifiedGraphName)),
        CTRelationship(Set("B"), Some(testQualifiedGraphName)),
        CTNode(Set("C"), Some(testQualifiedGraphName))
      )
      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (c1:C)<-[b1:B]-(a:A)-[b2:B]->(c2:C) RETURN a""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))

      optimizedPlan.occourences[ValueJoin] should be(1)
      optimizedPlan.occourences[PatternScan] should be(2)

      optimizedPlan.exists {
        case _: Expand => true
        case _ => false
      } should be(false)
      optimizedPlan.exists {
        case PatternScan(otherPattern, map, _, _) =>
          pattern == otherPattern &&
            map == Map(Var("a")(CTNode) -> pattern.sourceEntity, Var("b1")(CTRelationship) -> pattern.relEntity, Var("c1")(CTNode) -> pattern.targetEntity)
        case _ => false
      } should be(true)

      optimizedPlan.exists {
        case PatternScan(otherPattern, map, _, _) =>
          pattern == otherPattern &&
            map == Map(Var("a")(CTNode) -> pattern.sourceEntity, Var("b2")(CTRelationship) -> pattern.relEntity, Var("c2")(CTNode) -> pattern.targetEntity)
        case _ => false
      } should be(true)
    }

    it("does not insert node rel patterns if not all node label combos are covered") {
      val pattern = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))
      val schema = Schema.empty
        .withNodePropertyKeys("Person")()
        .withNodePropertyKeys("Person", "Employee")()
        .withRelationshipPropertyKeys("KNOWS")()

      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (a:Person)-[:KNOWS]->(:Person) RETURN a""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))
      optimizedPlan.occourences[PatternScan] should be(2)

      optimizedPlan.exists {
        case PatternScan(_: NodeRelPattern, _, _, _) => true
        case _ => false
      } should be(false)
    }

    it("does not insert node rel patterns if not all rel types are covered") {
      val pattern = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))
      val schema = Schema.empty
        .withNodePropertyKeys("Person")()
        .withRelationshipPropertyKeys("KNOWS")()
        .withRelationshipPropertyKeys("LOVES")()

      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (a:Person)-[:KNOWS|LOVES]->(:Person) RETURN a""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))
      optimizedPlan.occourences[PatternScan] should be(2)
      optimizedPlan.occourences[ValueJoin] should be(0)
    }

    it("does not insert triplet patterns if not all node label combos are covered") {
      val pattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))
      val schema = Schema.empty
        .withNodePropertyKeys("Person")()
        .withNodePropertyKeys("Person", "Employee")()
        .withRelationshipPropertyKeys("KNOWS")()

      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (a:Person)-[:KNOWS]->(:Person) RETURN a""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))
      optimizedPlan.occourences[PatternScan] should be(2)

      optimizedPlan.exists {
        case PatternScan(_: TripletPattern, _, _, _) => true
        case _ => false
      } should be(false)
    }

    it("does not insert triplet patterns if not all rel types are covered") {
      val pattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))
      val schema = Schema.empty
        .withNodePropertyKeys("Person")()
        .withRelationshipPropertyKeys("KNOWS")()
        .withRelationshipPropertyKeys("LOVES")()

      val graph = TestGraph(schema, Set(pattern))

      val plan = logicalPlan(
        """MATCH (a:Person)-[:KNOWS|LOVES]->(:Person) RETURN a""",
        graph
      )

      val optimizedPlan = LogicalOptimizer(plan)(plannerContext(graph))
      optimizedPlan.occourences[PatternScan] should be(2)

      optimizedPlan.exists {
        case PatternScan(_: TripletPattern, _, _, _) => true
        case _ => false
      } should be(false)
    }
  }

  def plannerContext(graph: PropertyGraph): LogicalPlannerContext = {
    val catalog = QueryLocalCatalog
      .empty
      .withGraph(testQualifiedGraphName, graph)
      .withSchema(testQualifiedGraphName, graph.schema)

    LogicalPlannerContext(
      graph.schema,
      Set.empty,
      Map.empty,
      catalog
    )
  }

  private def logicalPlan(query: String, graph: PropertyGraph): LogicalOperator = {
    val producer = new LogicalOperatorProducer
    val logicalPlanner = new LogicalPlanner(producer)
    val ir = query.asCypherQuery(testGraphName -> graph.schema)(graph.schema)

    val logicalPlannerContext = plannerContext(graph)

    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logicalPlan
  }

  case class TestGraph(
    override val schema: Schema,
    override val patterns: Set[Pattern] = Set.empty
  ) extends PropertyGraph {

    override def session: CypherSession = ???

    override def nodes(
      name: String,
      nodeCypherType: CTNode,
      exactLabelMatch: Boolean
    ): CypherRecords = ???

    override def relationships(
      name: String,
      relCypherType: CTRelationship
    ): CypherRecords = ???

    override def unionAll(others: PropertyGraph*): PropertyGraph = ???
  }
}
