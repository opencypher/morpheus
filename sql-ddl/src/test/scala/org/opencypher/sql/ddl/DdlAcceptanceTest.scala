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
package org.opencypher.sql.ddl

import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.sql.ddl.DdlParser._

class DdlAcceptanceTest extends BaseTestSuite {

  describe("DDL to OKAPI schema") {
    it("can construct schema with node label") {
      val gName = "test"

      val ddl =
        s"""CATALOG CREATE LABEL (A {name: STRING})
           |
           |CREATE GRAPH SCHEMA foo
           |  (A)
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with edge label") {
      val gName = "test"

      val ddl =
        s"""CATALOG CREATE LABEL (A {name: STRING})
           |
           |CREATE GRAPH SCHEMA foo
           |  [A]
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withRelationshipPropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with node and edge labels") {
      val gName = "test"

      val ddl =
        s"""
           |CATALOG CREATE LABEL (Node {val: String})
           |CATALOG CREATE LABEL (REL {name: STRING})
           |
           |CREATE GRAPH SCHEMA foo
           |  (Node) [REL]
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin


      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
      )
    }

    it("can construct schema with node labels with element key") {
      val gName = "test"

      val ddl =
        s"""
           |CATALOG CREATE LABEL (Node {val: String, another : String})
           |   KEY akey (val)
           |
           |CREATE GRAPH SCHEMA foo
           |  (Node)
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString, "another" -> CTString)
          .withNodeKey("Node", Set("val"))
      )
    }

    it("can construct schema with single NEN pattern") {
      val gName = "test"

      val ddl =
        s"""|CATALOG CREATE LABEL (Node {val: String})
            |CATALOG CREATE LABEL (REL {name: STRING})
            |
            |CREATE GRAPH SCHEMA foo
            |  (Node)-[REL]->(Node)
            |
            |CREATE GRAPH $gName WITH SCHEMA foo""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))

      )
    }

    it("can construct schema with single NEN pattern 2") {
      val gName = "test"

      val ddl =
        s"""|CATALOG CREATE LABEL (Node {val: String})
            |CATALOG CREATE LABEL (REL {name: STRING})
            |
            |CREATE GRAPH SCHEMA foo
            | (Node)-[REL]->(Node)
            |
            |CREATE GRAPH $gName WITH SCHEMA foo""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))

      )
    }

    it("can combine local and global labels") {
      // Given
      val ddl =
        """
          |CATALOG CREATE LABEL (MyLabel {property: STRING, data: INTEGER?})
          |CATALOG CREATE LABEL (REL_TYPE1 {property: BOOLEAN})
          |CATALOG CREATE LABEL (REL_TYPE2)
          |
          |CREATE GRAPH SCHEMA mySchema
          |  -- local label declarations
          |  LABEL (LocalLabel1 {property: STRING}),
          |  LABEL (LocalLabel2),
          |
          |  -- label set declarations
          |  (LocalLabel1, LocalLabel2),
          |  (LocalLabel1),
          |  (MyLabel),
          |
          |  [REL_TYPE1],
          |  [REL_TYPE2]
          |
          |  -- schema patterns
          |  (MyLabel) <0..*> -[REL_TYPE1]-> <1> (LocalLabel1),
          |  (LocalLabel1, LocalLabel2)-[REL_TYPE2]->(MyLabel)
        """.stripMargin

      parse(ddl).globalSchemas("mySchema") should equal(
        Schema.empty
          .withNodePropertyKeys("MyLabel")("property" -> CTString, "data" -> CTInteger.nullable)
          .withNodePropertyKeys("LocalLabel1")("property" -> CTString)
          .withNodePropertyKeys("LocalLabel1", "LocalLabel2")("property" -> CTString)
          .withRelationshipPropertyKeys("REL_TYPE1")("property" -> CTBoolean)
          .withRelationshipPropertyKeys("REL_TYPE2")()
          .withSchemaPatterns(SchemaPattern(Set("MyLabel"), "REL_TYPE1", Set("LocalLabel1")))
          .withSchemaPatterns(SchemaPattern(Set("LocalLabel1", "LocalLabel2"), "REL_TYPE2", Set("MyLabel")))
      )
    }
  }
  //
  //  describe("mapping errors") {
  //    it("throws error when table is missing a property column") {
  //      val viewName = "view_A"
  //
  //      val ddl =
  //        s"""
  //           |CATALOG CREATE LABEL (A {name: STRING, age: INTEGER})
  //           |
  //           |CREATE GRAPH SCHEMA mySchema
  //           |
  //           |  (A);
  //           |
  //           |CREATE GRAPH myGraph WITH SCHEMA mySchema
  //           |
  //           |  NODE LABEL SETS (
  //           |    (A) FROM $viewName
  //           |  )
  //      """.stripMargin
  //
  //
  //      an[IllegalArgumentException] should be thrownBy {
  //        process(ddl, Map(viewName -> Map("name" -> "varchar(8)")))
  //      }
  //    }
  //
  //    it("throws error when table is missing a nullable property column") {
  //      val viewName = "view_A"
  //      val graphName = "myGraph"
  //
  //      val ddl =
  //        s"""
  //           |CATALOG CREATE LABEL (A {name: STRING, age: INTEGER})
  //           |
  //           |CREATE GRAPH SCHEMA mySchema
  //           |
  //           |  (A);
  //           |
  //           |CREATE GRAPH $graphName WITH SCHEMA mySchema
  //           |
  //           |  NODE LABEL SETS(
  //           |    (A) FROM $viewName
  //           |  )
  //      """.stripMargin
  //
  //      an[IllegalArgumentException] should be thrownBy {
  //        process(ddl, Map(viewName -> Map("name" -> "varchar(8)")))
  //      }
  //    }
  //
  //    it("reports when using same table for many node labels") {
  //      val ddl =
  //        """
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL L1, LABEL L2  -- define labels
  //          |
  //          |    (L1), (L2)     -- include them in combinations
  //          |  )
  //          |
  //          |
  //          |  NODE LABEL SETS (
  //          |    (L1) FROM myView,   -- map from same view
  //          |    (L2) FROM myView   -- as this one
  //          |  )
  //        """.stripMargin
  //
  //      an [IllegalArgumentException] shouldBe thrownBy(process(ddl, Map("myView" -> Map.empty)))
  //    }
  //  }
  //
  //  describe("mapping syntax") {
  //    it("can map a node label") {
  //      val ddl =
  //        """
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL L1
  //          |
  //          |    (L1)
  //          |  )
  //          |
  //          |  NODE LABEL SETS ( (L1) FROM myView )
  //        """.stripMargin
  //
  //      val tableMetaData = Map("myView" -> Map.empty[String, String])
  //      val manager = process(ddl, tableMetaData)
  //
  //      val labelCombo = new LabelSetImpl(util.Arrays.asList(label("L1")))
  //      val sqlTableName = new QualifiedSqlNameImpl("", "", "myView")
  //      manager.getGraphMappingManager.getOne(qgn("foo")).getNodeMappings.get(set("L1")) should equal(
  //        new NodeMappingImpl(labelCombo, new NodeSourceMappingImpl(labelCombo, sqlTableName, map(), meta(sqlTableName)))
  //      )
  //    }
  //
  //    it("can map from a wider table") {
  //      val ddl =
  //        """
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL (L1 {p1: STRING})
  //          |
  //          |    (L1)
  //          |  )
  //          |
  //          |  NODE LABEL SETS ( (L1) FROM myView )
  //        """.stripMargin
  //
  //      val columns = Map("p1" -> "", "p2" -> "", "p" -> "")
  //      val loading = processLoading(ddl, qgn("foo"), Map("myView" -> columns))
  //
  //      val labelCombo = new LabelSetImpl(list(label("L1")))
  //      val myView = tbl("myView")
  //
  //      loading.getGraphMapping.getNodeMappings.get(set("L1")) should equal(
  //        new NodeMappingImpl(labelCombo, new NodeSourceMappingImpl(labelCombo, myView, map("p1" -> "p1"), meta(myView, columns)))
  //      )
  //      loading.getNodeLoaderMap.asScala(set("L1")).asScala.map(_.getSql) should equal(List(
  //        "SELECT p1 FROM .myView"
  //      ))
  //    }
  //
  //    it("renames columns to property names") {
  //      val ddl =
  //        """
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL (L1 {p1: STRING})
  //          |
  //          |    (L1)
  //          |  )
  //          |
  //          |  NODE LABEL SETS ( (L1) FROM myView (column AS p1) )
  //        """.stripMargin
  //
  //      val columns = Map("column" -> "")
  //      val loading = processLoading(ddl, qgn("foo"), Map("myView" -> columns))
  //
  //      val labelCombo = new LabelSetImpl(list(label("L1")))
  //      val myView = tbl("myView")
  //
  //      loading.getGraphMapping.getNodeMappings.get(set("L1")) should equal(
  //        new NodeMappingImpl(labelCombo, new NodeSourceMappingImpl(labelCombo, myView, map("p1" -> "column"), meta(myView, columns)))
  //      )
  //      loading.getNodeLoaderMap.asScala(set("L1")).asScala.map(_.getSql) should equal(List(
  //        "SELECT column AS p1 FROM .myView"
  //      ))
  //    }
  //
  //    it("can map a node label with set schema") {
  //      val ddl =
  //        """
  //          |SET SCHEMA foo.bar
  //          |
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL L1
  //          |
  //          |    (L1)
  //          |  )
  //          |
  //          |  NODE LABEL SETS ( (L1) FROM myView )
  //        """.stripMargin
  //
  //      val tableMetaData = Map("myView" -> Map.empty[String, String])
  //      val manager = process(ddl, tableMetaData)
  //
  //      val labelCombo = new LabelSetImpl(util.Arrays.asList(label("L1")))
  //      val sqlTableName = new QualifiedSqlNameImpl("foo", "bar", "myView")
  //      manager.getGraphMappingManager.getOne(qgn("foo")).getNodeMappings.get(set("L1")) should equal(
  //        new NodeMappingImpl(labelCombo, new NodeSourceMappingImpl(labelCombo, sqlTableName, map(), meta(sqlTableName)))
  //      )
  //    }
  //
  //    it("can map a relationship") {
  //      val ddl =
  //        """
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL L
  //          |    LABEL T
  //          |
  //          |    (L)-[T]->(L)
  //          |  )
  //          |
  //          |  NODE LABEL SETS ( (L) FROM myView1 )
  //          |
  //          |  RELATIONSHIP LABEL SETS (
  //          |    [T] FROM myView2 relAlias
  //          |      START NODES
  //          |        LABEL SET (L)
  //          |        FROM myView1 startAlias
  //          |          JOIN ON startAlias.nodeCol = relAlias.relCol1
  //          |      END NODES
  //          |        LABEL SET (L)
  //          |        FROM myView1 endAlias
  //          |          JOIN ON endAlias.nodeCol = relAlias.relCol2
  //          |  )
  //          |
  //        """.stripMargin
  //
  //      val tableMetaData = Map("myView1" -> Map("nodeCol" -> "varchar(1)"), "myView2" -> Map("relCol1" -> "char(2)", "relCol1" -> "char(2)"))
  //      val manager = process(ddl, tableMetaData)
  //
  //      val nodeLabel = new LabelSetImpl(list(label("L")))
  //      val relLabel = new LabelSetImpl(list(label("T")))
  //      val nodeTable = new QualifiedSqlNameImpl("", "", "myView1")
  //      val nodeTableMeta = TestMetaDataFinder(tableMetaData).getTableMetaData(nodeTable)
  //      val relTable = new QualifiedSqlNameImpl("", "", "myView2")
  //      val relTableMeta = TestMetaDataFinder(tableMetaData).getTableMetaData(relTable)
  //      val graphMapping = manager.getGraphMappingManager.getOne(qgn("foo"))
  //
  //      graphMapping.getNodeMappings.get(set("L")) should equal(
  //        new NodeMappingImpl(nodeLabel, new NodeSourceMappingImpl(nodeLabel, nodeTable, map(), nodeTableMeta))
  //      )
  //
  //      val startSide = new EdgeSideMappingImpl(nodeTable, nodeLabel, map("nodeCol" -> "relCol1"), map(), false, false)
  //      val endSide = new EdgeSideMappingImpl(nodeTable, nodeLabel, map("nodeCol" -> "relCol2"), map(), false, false)
  //
  //      graphMapping.getEdgeMappings should contain (new EdgeMappingImpl(relLabel, list(new EdgeSourceMappingImpl(relTable, map(), startSide, endSide, relTableMeta))))
  //    }
  //
  //    it("doc example test") {
  //      val ddl =
  //        """
  //          |CATALOG CREATE LABEL (Cat {name: STRING, lives: INTEGER?})
  //          |CATALOG CREATE LABEL (HUNTS {eats: BOOLEAN})
  //          |CATALOG CREATE LABEL (OWNS)
  //          |
  //          |CREATE GRAPH SCHEMA catsAndPrey
  //          |  -- local label definitions
  //          |  LABEL (Prey {species: STRING}),
  //          |  LABEL (Person),
  //          |
  //          |  -- label set declarations
  //          |  (Prey, Person),
  //          |  (Prey),
  //          |  (Cat),
  //          |
  //          |  -- relationship type declarations
  //          |  [HUNTS],
  //          |  [OWNS]
  //          |
  //          |  -- schema patterns
  //          |  (Cat) <0..*> -[HUNTS]-> <1> (Prey),
  //          |  (Prey, Person)-[OWNS]->(Cat)
  //          |
  //          |CREATE GRAPH cats WITH SCHEMA catsAndPrey
  //          |
  //          |  NODE LABEL SETS (
  //          |    (Cat) FROM catTable,
  //          |    (Prey) FROM animalPrey,
  //          |    (Prey, Person) FROM people (nationality AS species)
  //          |  )
  //          |
  //          |  RELATIONSHIP LABEL SETS (
  //          |    [HUNTS]
  //          |      FROM myTable4 rel
  //          |        START NODES
  //          |          LABEL SET (Cat) FROM catTable start
  //          |          JOIN ON start.catId = rel.column1
  //          |        END NODES
  //          |          LABEL SET (Prey) FROM animalPrey end
  //          |          JOIN ON end.species = rel.column2,
  //          |
  //          |    [OWNS]
  //          |      FROM myTable5 rel
  //          |        START NODES
  //          |          LABEL SET (Prey, Person) FROM people start
  //          |          JOIN ON start.column = rel.start
  //          |        END NODES
  //          |          LABEL SET (Cat) FROM catTable end
  //          |          JOIN ON end.catId = rel.end
  //          |  )
  //          |
  //        """.stripMargin
  //
  //      val fakeTables = Map(
  //        "catTable" -> Map("name" -> "", "lives" -> "", "catId" -> ""),
  //        "animalPrey" -> Map("species" -> ""),
  //        "people" -> Map("nationality" -> ""),
  //        "myTable4" -> Map("column1" -> "", "column2" -> "", "eats" -> ""),
  //        "myTable5" -> Map("start" -> "", "end" -> "")
  //      )
  //      val finder = TestMetaDataFinder(fakeTables)
  //      val manager = process(ddl, fakeTables)
  //
  //      val cat = labelSet(label("Cat", Map("name" -> "STRING", "lives" -> "INTEGER?")))
  //      val prey = labelSet(label("Prey", Map("species" -> "STRING")))
  //      val humans = labelSet(label("Prey", Map("species" -> "STRING")), label("Person"))
  //
  //      val mapping = manager.getGraphMappings.asScala(qgn("cats"))
  //
  //      mapping.getNodeMappings.asScala should equal(Map(
  //        set("Cat") -> new NodeMappingImpl(cat, new NodeSourceMappingImpl(cat, tbl("catTable"), map("name" -> "name", "lives" -> "lives"), finder.getTableMetaData(tbl("catTable")))),
  //        set("Prey") -> new NodeMappingImpl(prey, new NodeSourceMappingImpl(prey, tbl("animalPrey"), map("species" -> "species"), finder.getTableMetaData(tbl("animalPrey")))),
  //        set("Prey", "Person") -> new NodeMappingImpl(humans, new NodeSourceMappingImpl(humans, tbl("people"), map("species" -> "nationality"), finder.getTableMetaData(tbl("people"))))
  //      ))
  //
  //      val join1 = new EdgeSideMappingImpl(tbl("catTable"), cat, map("catId" -> "column1"), map(), false, false)
  //      val join2 = new EdgeSideMappingImpl(tbl("animalPrey"), prey, map("species" -> "column2"), map(), false, false)
  //      val join3 = new EdgeSideMappingImpl(tbl("people"), humans, map("column" -> "start"), map(), false, false)
  //      val join4 = new EdgeSideMappingImpl(tbl("catTable"), cat, map("catId" -> "end"), map(), false, false)
  //
  //      mapping.getEdgeMappings.asScala should equal(Set(
  //        new EdgeMappingImpl(labelSet(label("HUNTS", Map("eats" -> "BOOLEAN"))), list(new EdgeSourceMappingImpl(tbl("myTable4"), map("eats" -> "eats"), join1, join2, finder.getTableMetaData(tbl("myTable4"))))),
  //        new EdgeMappingImpl(labelSet(label("OWNS")), list(new EdgeSourceMappingImpl(tbl("myTable5"), map(), join3, join4, finder.getTableMetaData(tbl("myTable5")))))
  //      ))
  //    }
  //
  //    it("can map two graphs") {
  //      val ddl =
  //        """
  //          |SET SCHEMA DB.SCHEMA
  //          |
  //          |CATALOG CREATE LABEL (A {email: STRING, id: INTEGER})
  //          |CATALOG CREATE LABEL (B {data: STRING, number: FLOAT})
  //          |CATALOG CREATE LABEL (C)
  //          |
  //          |CATALOG CREATE LABEL (REL1)
  //          |CATALOG CREATE LABEL (REL2 {since: INTEGER})
  //          |
  //          |CREATE GRAPH SCHEMA mySchema
  //          |
  //          |  (A)-[REL1]->(B),
  //          |  (B)-[REL1]->(C),
  //          |  (A)-[REL2]->(C)
  //          |
  //          |CREATE GRAPH g1 WITH SCHEMA mySchema
  //          |
  //          |  NODE LABEL SETS ( (A) FROM a_big, (B) FROM b_big, (C) FROM c_big )
  //          |
  //          |  RELATIONSHIP LABEL SETS (
  //          |    [REL1]
  //          |      FROM ab relAlias
  //          |        START NODES
  //          |          LABEL SET (A)
  //          |          FROM a_big startAlias
  //          |            JOIN ON startAlias.email = relAlias.email
  //          |        END NODES
  //          |          LABEL SET (B)
  //          |          FROM b_big endAlias
  //          |            JOIN ON endAlias.number = relAlias.number,
  //          |
  //          |      FROM bc relAlias
  //          |        START NODES
  //          |          LABEL SET (B)
  //          |          FROM b_big startAlias
  //          |            JOIN ON startAlias.number = relAlias.number
  //          |        END NODES
  //          |          LABEL SET (C)
  //          |          FROM c_big endAlias
  //          |            JOIN ON endAlias.hidden_id = relAlias.hidden_id,
  //          |
  //          |    [REL2]
  //          |      FROM ac relAlias
  //          |        START NODES
  //          |          LABEL SET (A)
  //          |          FROM a_big startAlias
  //          |            JOIN ON startAlias.email = relAlias.email
  //          |        END NODES
  //          |          LABEL SET (C)
  //          |          FROM c_big endAlias
  //          |            JOIN ON endAlias.hidden_id = relAlias.hidden_id
  //          |  )
  //          |
  //          |CREATE GRAPH g2 WITH SCHEMA mySchema
  //          |
  //          |  NODE LABEL SETS ( (A) FROM a_small, (B) FROM b_small, (C) FROM c_small )
  //          |
  //          |  RELATIONSHIP LABEL SETS (
  //          |    [REL1]
  //          |      FROM ab relAlias
  //          |        START NODES
  //          |          LABEL SET (A)
  //          |          FROM a_small startAlias
  //          |            JOIN ON startAlias.email = relAlias.email
  //          |        END NODES
  //          |          LABEL SET (B)
  //          |          FROM b_small endAlias
  //          |            JOIN ON endAlias.number = relAlias.number,
  //          |
  //          |      FROM bc relAlias
  //          |        START NODES
  //          |          LABEL SET (B)
  //          |          FROM b_small startAlias
  //          |            JOIN ON startAlias.number = relAlias.number
  //          |        END NODES
  //          |          LABEL SET (C)
  //          |          FROM c_small endAlias
  //          |            JOIN ON endAlias.hidden_id = relAlias.hidden_id,
  //          |
  //          |    [REL2]
  //          |      FROM ac relAlias
  //          |        START NODES
  //          |          LABEL SET (A)
  //          |          FROM a_small startAlias
  //          |            JOIN ON startAlias.email = relAlias.email
  //          |        END NODES
  //          |          LABEL SET (C)
  //          |          FROM c_small endAlias
  //          |            JOIN ON endAlias.hidden_id = relAlias.hidden_id
  //          |  )
  //          |
  //          |
  //        """.stripMargin
  //
  //      val fakeTables = Map(
  //        "a_big" -> Map("email" -> "", "id" -> ""),
  //        "a_small" -> Map("email" -> "", "id" -> ""),
  //        "b_big" -> Map("data" -> "", "number" -> ""),
  //        "b_small" -> Map("data" -> "", "number" -> ""),
  //        "c_big" -> Map("hidden_id" -> ""),
  //        "c_small" -> Map("hidden_id" -> ""),
  //        "ab" -> Map("email" -> "", "number" -> ""),
  //        "bc" -> Map("number" -> "", "hidden_id" -> ""),
  //        "ac" -> Map("email" -> "", "hidden_id" -> "", "since" -> "")
  //      )
  //      val manager = process(ddl, fakeTables)
  //      val finder = TestMetaDataFinder(fakeTables)
  //
  //      val a = new LabelSetImpl(list(label("A", Map("email" -> "STRING", "id" -> "INTEGER"))))
  //      val b = new LabelSetImpl(list(label("B", Map("data" -> "STRING", "number" -> "FLOAT"))))
  //      val c = new LabelSetImpl(list(label("C")))
  //      val a_big = tbl("a_big", "SCHEMA", "DB")
  //      val b_big = tbl("b_big", "SCHEMA", "DB")
  //      val c_big = tbl("c_big", "SCHEMA", "DB")
  //
  //      val rel1 = new LabelSetImpl(list(label("REL1")))
  //      val rel2 = new LabelSetImpl(list(label("REL2", Map("since" -> "INTEGER"))))
  //      val ab = tbl("ab", "SCHEMA", "DB")
  //      val bc = tbl("bc", "SCHEMA", "DB")
  //      val ac = tbl("ac", "SCHEMA", "DB")
  //      val g1 = manager.getGraphMappingManager.getOne(qgn("g1"))
  //
  //      g1.getNodeMappings.asScala should equal(Map(
  //        set("A") -> new NodeMappingImpl(a, new NodeSourceMappingImpl(a, a_big, map("email" -> "email", "id" -> "id"), finder.getTableMetaData(a_big))),
  //        set("B") -> new NodeMappingImpl(b, new NodeSourceMappingImpl(b, b_big, map("data" -> "data", "number" -> "number"), finder.getTableMetaData(b_big))),
  //        set("C") -> new NodeMappingImpl(c, new NodeSourceMappingImpl(c, c_big, map(), finder.getTableMetaData(c_big)))
  //      ))
  //
  //      val joinA = new EdgeSideMappingImpl(a_big, a, map("email" -> "email"), map(), false, false)
  //      val joinB = new EdgeSideMappingImpl(b_big, b, map("number" -> "number"), map(), false, false)
  //      val joinC = new EdgeSideMappingImpl(c_big, c, map("hidden_id" -> "hidden_id"), map(), false, false)
  //
  //      g1.getEdgeMappings.asScala should equal(Set(
  //        new EdgeMappingImpl(rel2, list(new EdgeSourceMappingImpl(ac, map("since" -> "since"), joinA, joinC, finder.getTableMetaData(ac)))),
  //        new EdgeMappingImpl(rel1, list(
  //          new EdgeSourceMappingImpl(ab, map(), joinA, joinB, finder.getTableMetaData(ab)),
  //          new EdgeSourceMappingImpl(bc, map(), joinB, joinC, finder.getTableMetaData(bc))
  //        ))
  //      ))
  //
  //      val a_small = tbl("a_small", "SCHEMA", "DB")
  //      val b_small = tbl("b_small", "SCHEMA", "DB")
  //      val c_small = tbl("c_small", "SCHEMA", "DB")
  //
  //      val g2 = manager.getGraphMappingManager.getOne(qgn("g2"))
  //
  //      g2.getNodeMappings.asScala should equal(Map(
  //        set("A") -> new NodeMappingImpl(a, new NodeSourceMappingImpl(a, a_small, map("email" -> "email", "id" -> "id"), finder.getTableMetaData(a_small))),
  //        set("B") -> new NodeMappingImpl(b, new NodeSourceMappingImpl(b, b_small, map("data" -> "data", "number" -> "number"), finder.getTableMetaData(b_small))),
  //        set("C") -> new NodeMappingImpl(c, new NodeSourceMappingImpl(c, c_small, map(), finder.getTableMetaData(c_small)))
  //      ))
  //
  //      val joinA_small = new EdgeSideMappingImpl(a_small, a, map("email" -> "email"), map(), false, false)
  //      val joinB_small = new EdgeSideMappingImpl(b_small, b, map("number" -> "number"), map(), false, false)
  //      val joinC_small = new EdgeSideMappingImpl(c_small, c, map("hidden_id" -> "hidden_id"), map(), false, false)
  //
  //      g2.getEdgeMappings.asScala should equal(Set(
  //        new EdgeMappingImpl(rel2, list(new EdgeSourceMappingImpl(ac, map("since" -> "since"), joinA_small, joinC_small, finder.getTableMetaData(ac)))),
  //        new EdgeMappingImpl(rel1, list(
  //          new EdgeSourceMappingImpl(ab, map(), joinA_small, joinB_small, finder.getTableMetaData(ab)),
  //          new EdgeSourceMappingImpl(bc, map(), joinB_small, joinC_small, finder.getTableMetaData(bc))
  //        ))
  //      ))
  //    }
  //
  //    it("generates correct SQL") {
  //      val ddl =
  //        """
  //          |CREATE GRAPH foo
  //          |  WITH SCHEMA (
  //          |    LABEL (L {nodeCol: STRING, age: INTEGER} KEY k (age))
  //          |    LABEL (T {relCol2: STRING})
  //          |
  //          |    (L)-[T]->(L)
  //          |  )
  //          |
  //          |  NODE LABEL SETS ( (L) FROM myView1 )
  //          |
  //          |  RELATIONSHIP LABEL SETS (
  //          |    [T] FROM myView2 relAlias
  //          |      START NODES
  //          |        LABEL SET (L)
  //          |        FROM myView1 startAlias
  //          |          JOIN ON startAlias.nodeCol = relAlias.relCol1
  //          |      END NODES
  //          |        LABEL SET (L)
  //          |        FROM myView1 endAlias
  //          |          JOIN ON endAlias.age = relAlias.relCol2
  //          |  )
  //          |
  //        """.stripMargin
  //
  //      val tableMetaData = Map("myView1" -> Map("nodeCol" -> "varchar(1)", "age" -> "int"), "myView2" -> Map("relCol1" -> "char(2)", "relCol2" -> "char(2)"))
  //      val loading = processLoading(ddl, qgn("foo"), tableMetaData)
  //
  //      loading.getNodeLoaderMap.asScala(set("L")).asScala.map(_.getSql) should equal(List(
  //        "SELECT nodeCol,age, nodeCol AS KEY_1, age AS KEY_2 FROM .myView1"
  //      ))
  //      loading.getEdgeLoaders.asScala.map(_.getSql) should equal(Set(
  //        "SELECT relCol2, relCol1 AS START_KEY_1, relCol2 AS END_KEY_2 FROM .myView2"
  //      ))
  //    }
  //  }
  //
  //  describe("misc") {
  //    it("reports bad ddl") {
  //      val ddl = "this is not a ddl"
  //
  //      a[DdlSyntaxException] should be thrownBy {
  //        process(ddl)
  //      }
  //    }
  //  }
  //
  //  private def process(ddl: String, tables: Map[String, Map[String, String]] = Map.empty) = {
  //    val storeManager = new ActiveStoreManager()
  //    val ddlProcessor = new GraphDDLProcessor(storeManager, TestMetaDataFinder(tables))
  //    ddlProcessor.processStream(new ByteArrayInputStream(ddl.getBytes))
  //    storeManager
  //  }
  //
  //  private def processLoading(ddl: String, graphName: QualifiedGrName, tables: Map[String, Map[String, String]] = Map.empty) = {
  //    val storeManager = new ActiveStoreManager()
  //    val metaDataFinder = TestMetaDataFinder(tables)
  //    val ddlProcessor = new GraphDDLProcessor(storeManager, metaDataFinder)
  //    ddlProcessor.processStream(new ByteArrayInputStream(ddl.getBytes))
  //    val mapping = storeManager.getGraphMappingManager.getOne(graphName)
  //    new GraphLoadingImpl(mapping, metaDataFinder)
  //  }
  //
  //  // assume anonymous container name will work
  //  private def qgn(name: String) = new QualifiedGrNameImpl("", name)
  //
  //  private def tbl(tableName: String, schemaName: String = "", datasourceName: String = "") =
  //    new QualifiedSqlNameImpl(datasourceName, schemaName, tableName)
  //
  //  def map[S, T](tuples: (S, T)*) : util.Map[S, T] = {
  //    val javaMap = new util.HashMap[S, T]()
  //    tuples.foreach {
  //      case (k, v) => javaMap.put(k, v)
  //    }
  //    javaMap
  //  }
  //
  //  private def meta(tableName: QualifiedSqlName, columns: Map[String, String] = Map.empty): TableMetaData =
  //    TestTableMetaData(tableName, columns)
  //
  //  private def label(name: String, properties: Map[String, String] = Map.empty, keys: Map[String, util.List[String]] = Map.empty): Label = {
  //    new LabelImpl(qgn(name), map(properties.toList: _*), map(keys.toList: _*))
  //  }
  //
  //  private def labelSet(labels: Label*): LabelSet = {
  //    new LabelSetImpl(list(labels: _*))
  //  }
  //
  //  private def set[T](elems: T*): util.HashSet[T] = {
  //    val set = new util.HashSet[T]()
  //    elems.foreach(set.add)
  //    set
  //  }
  //
  //  private def list[T](elems: T*): util.List[T] = {
  //    val list = new util.ArrayList[T]()
  //    elems.foreach(list.add)
  //    list
  //  }
}
