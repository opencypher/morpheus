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
package org.opencypher.okapi.api.types

import org.opencypher.okapi.ApiBaseTest
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.impl.types.CypherTypeParser.parseCypherType

import scala.language.postfixOps

class CypherTypesTest extends ApiBaseTest {

  val materialTypes: Seq[MaterialCypherType] = Seq(
    CTAny,
    CTBoolean,
    CTNumber,
    CTInteger,
    CTFloat,
    CTBigDecimal(12),
    CTString,
    CTMap(Map("foo" -> CTString, "bar" -> CTInteger)),
    CTNode,
    CTNode("Person"),
    CTNode("Person", "Employee"),
    CTRelationship,
    CTRelationship("KNOWS"),
    CTRelationship("KNOWS", "LOVES"),
    CTPath,
    CTList(CTAny),
    CTList(CTList(CTBoolean)),
    CTList(CTString.nullable),
    CTVoid
  )

  val nullableTypes: Seq[NullableCypherType] =
    materialTypes.map(_.nullable)

  val allTypes: Seq[CypherType] =
    materialTypes ++ nullableTypes

  it("couldBe") {
    CTAny couldBeSameTypeAs CTNode shouldBe true
    CTNode couldBeSameTypeAs CTAny shouldBe true
    CTInteger couldBeSameTypeAs CTNumber shouldBe true
    CTNumber couldBeSameTypeAs CTInteger shouldBe true
    CTFloat couldBeSameTypeAs CTInteger shouldBe false
    CTBoolean couldBeSameTypeAs CTInteger shouldBe false

    CTRelationship couldBeSameTypeAs CTNode shouldBe false

    CTList(CTInteger) couldBeSameTypeAs CTList(CTFloat) shouldBe false
    CTList(CTInteger) couldBeSameTypeAs CTList(CTAny) shouldBe true
    CTList(CTAny) couldBeSameTypeAs CTList(CTInteger) shouldBe true

    CTNull couldBeSameTypeAs CTInteger.nullable shouldBe true
    CTInteger.nullable couldBeSameTypeAs CTNull shouldBe true

    CTMap couldBeSameTypeAs CTMap() shouldBe true
    CTMap() couldBeSameTypeAs CTMap shouldBe true
    CTMap(Map("name" -> CTString)) couldBeSameTypeAs CTMap shouldBe true
    CTMap couldBeSameTypeAs CTMap(Map("name" -> CTString)) shouldBe true
    CTMap(Map("name" -> CTString)) couldBeSameTypeAs CTMap() shouldBe false
    CTMap() couldBeSameTypeAs CTMap(Map("name" -> CTString)) shouldBe false

    CTNumber couldBeSameTypeAs CTBigDecimal(1, 1)
    CTBigDecimal(1, 1) couldBeSameTypeAs CTNumber
  }

  it("intersects") {
    CTAny intersects CTNode shouldBe true
    CTNode intersects CTAny shouldBe true
    CTInteger intersects CTNumber shouldBe true
    CTNumber intersects CTInteger shouldBe true
    CTFloat intersects CTInteger shouldBe false
    CTBoolean intersects CTInteger shouldBe false

    CTRelationship intersects CTNode shouldBe false

    CTList(CTInteger) intersects CTList(CTFloat) shouldBe true
    CTList(CTInteger) intersects CTList(CTAny) shouldBe true
    CTList(CTAny) intersects CTList(CTInteger) shouldBe true

    CTNull intersects CTInteger.nullable shouldBe true
    CTInteger.nullable intersects CTNull shouldBe true

    CTString.nullable intersects CTInteger.nullable shouldBe true
    CTNode.nullable intersects CTBoolean.nullable shouldBe true

    CTVoid intersects CTBoolean shouldBe false
    CTVoid intersects CTBoolean.nullable shouldBe false
    CTVoid intersects CTAny shouldBe false
    CTVoid intersects CTBoolean.nullable shouldBe false
  }

  it("joining with list of void") {
    val voidList = CTList(CTVoid)
    val otherList = CTList(CTString).nullable

    voidList join otherList should equal(otherList)
    otherList join voidList should equal(otherList)
  }

  it("type names") {
    Seq[(CypherType, (String, String))](
      CTAny -> ("ANY" -> "ANY?"),
      CTString -> ("STRING" -> "STRING?"),
      CTBoolean -> ("BOOLEAN" -> "BOOLEAN?"),
      CTNumber -> ("NUMBER" -> "NUMBER?"),
      CTInteger -> ("INTEGER" -> "INTEGER?"),
      CTFloat -> ("FLOAT" -> "FLOAT?"),
      CTBigDecimal(12,6) -> ("BIGDECIMAL(12,6)" -> "BIGDECIMAL(12,6)?"),
      CTMap(Map("foo" -> CTString, "bar" -> CTInteger)) -> ("MAP(foo: STRING, bar: INTEGER)" -> "MAP(foo: STRING, bar: INTEGER)?"),
      CTNode -> ("NODE" -> "NODE?"),
      CTNode("Person") -> ("NODE(:Person)" -> "NODE(:Person)?"),
      CTNode("Person", "Employee") -> ("NODE(:Person:Employee)" -> "NODE(:Person:Employee)?"),
      CTNode(Set("Person"), Some(QualifiedGraphName("foo.bar"))) -> ("NODE(:Person) @ foo.bar" -> "NODE(:Person) @ foo.bar?"),
      CTRelationship -> ("RELATIONSHIP" -> "RELATIONSHIP?"),
      CTRelationship(Set("KNOWS")) -> ("RELATIONSHIP(:KNOWS)" -> "RELATIONSHIP(:KNOWS)?"),
      CTRelationship(Set("KNOWS", "LOVES")) -> ("RELATIONSHIP(:KNOWS|:LOVES)" -> "RELATIONSHIP(:KNOWS|:LOVES)?"),
      CTRelationship(Set("KNOWS"), Some(QualifiedGraphName("foo.bar"))) -> ("RELATIONSHIP(:KNOWS) @ foo.bar" -> "RELATIONSHIP(:KNOWS) @ foo.bar?"),
      CTPath -> ("PATH" -> "PATH?"),
      CTList(CTInteger) -> ("LIST(INTEGER)" -> "LIST(INTEGER)?"),
      CTList(CTInteger.nullable) -> ("LIST(INTEGER?)" -> "LIST(INTEGER?)?"),
      CTDate -> ("DATE" -> "DATE?"),
      CTLocalDateTime -> ("LOCALDATETIME" -> "LOCALDATETIME?")
    ).foreach {
      case (t, (materialName, nullableName)) =>
        t.isNullable shouldBe false
        t.toString shouldBe materialName
        t.nullable.toString shouldBe nullableName
    }

    CTVoid.toString shouldBe "VOID"
    CTNull.toString shouldBe "NULL"
  }

  it("RELATIONSHIP type") {
    CTRelationship().superTypeOf(CTRelationship()) shouldBe true
    CTRelationship().superTypeOf(CTRelationship("KNOWS")) shouldBe true
    CTRelationship("KNOWS").superTypeOf(CTRelationship()) shouldBe false
    CTRelationship().subTypeOf(CTRelationship("KNOWS")) shouldBe false
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS")) shouldBe true
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS", "LOVES")) shouldBe false
    CTRelationship("KNOWS", "LOVES").superTypeOf(CTRelationship("LOVES")) shouldBe true
    CTRelationship("KNOWS").superTypeOf(CTRelationship("NOSE")) shouldBe false
  }

  it("RELATIONSHIP? type") {
    CTRelationshipOrNull().superTypeOf(CTRelationshipOrNull()) shouldBe true
    CTRelationshipOrNull().superTypeOf(CTRelationshipOrNull("KNOWS")) shouldBe true
    CTRelationshipOrNull("KNOWS").superTypeOf(CTRelationshipOrNull("KNOWS")) shouldBe true
    CTRelationshipOrNull("KNOWS").superTypeOf(CTRelationshipOrNull("KNOWS", "LOVES")) shouldBe false
    CTRelationshipOrNull("KNOWS", "LOVES").superTypeOf(CTRelationshipOrNull("LOVES")) shouldBe true
    CTRelationshipOrNull("KNOWS").superTypeOf(CTRelationshipOrNull("NOSE")) shouldBe false
    CTRelationshipOrNull("FOO").superTypeOf(CTNull) shouldBe true
  }

  it("NODE type") {
    CTNode().superTypeOf(CTNode()) shouldBe true
    CTNode().superTypeOf(CTNode("Person")) shouldBe true
    CTNode("Person").superTypeOf(CTNode()) shouldBe false
    CTNode().subTypeOf(CTNode("Person")) shouldBe false
    CTNode("Person").superTypeOf(CTNode("Person")) shouldBe true
    CTNode("Person").superTypeOf(CTNode("Person", "Employee")) shouldBe true
    CTNode("Person", "Employee").superTypeOf(CTNode("Employee")) shouldBe false
    CTNode("Person").superTypeOf(CTNode("Foo")) shouldBe false
    CTNode("Person").superTypeOf(CTNode) shouldBe false
  }

  it("NODE? type") {
    CTNodeOrNull().superTypeOf(CTNodeOrNull()) shouldBe true
    CTNodeOrNull().superTypeOf(CTNodeOrNull("Person")) shouldBe true
    CTNodeOrNull("Person").superTypeOf(CTNodeOrNull("Person")) shouldBe true
    CTNodeOrNull("Person").superTypeOf(CTNodeOrNull("Person", "Employee")) shouldBe true
    CTNodeOrNull("Person", "Employee").superTypeOf(CTNodeOrNull("Employee")) shouldBe false
    CTNodeOrNull("Person").superTypeOf(CTNodeOrNull("Foo")) shouldBe false
    CTNodeOrNull("Foo").superTypeOf(CTNull) shouldBe true
  }

  it("conversion between VOID and NULL") {
    CTVoid.nullable shouldBe CTNull
    CTNull.material shouldBe CTVoid
  }

  it("all nullable types contain their material types") {
    materialTypes.foreach(t => t.nullable superTypeOf t)
    materialTypes.foreach(t => t subTypeOf t.nullable)
  }

  it("conversion between material and nullable types") {
    materialTypes.foreach(t => t.nullable.material == t)
    nullableTypes.foreach(t => t.material.nullable == t)
  }

  it("subTypeOf as the inverse of superTypeOf") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        t1 subTypeOf t2 should be(t2 superTypeOf t1)
        t2 subTypeOf t1 should be(t1 superTypeOf t2)
      }
    }
  }

  it("basic type inheritance") {
    CTNumber superTypeOf CTInteger shouldBe true
    CTNumber superTypeOf CTFloat shouldBe true
    CTMap(Map("foo" -> CTAny, "bar" -> CTInteger)) superTypeOf CTMap(Map("foo" -> CTString, "bar" -> CTInteger)) shouldBe true
    CTMap(Map("foo" -> CTAny, "bar" -> CTAny)) superTypeOf CTMap(Map("foo" -> CTString, "bar" -> CTInteger)) shouldBe true

    CTAny superTypeOf CTInteger shouldBe true
    CTAny superTypeOf CTFloat shouldBe true
    CTAny superTypeOf CTNumber shouldBe true
    CTAny superTypeOf CTBoolean shouldBe true
    CTAny superTypeOf CTMap(Map()) shouldBe true
    CTAny superTypeOf CTNode shouldBe true
    CTAny superTypeOf CTRelationship shouldBe true
    CTAny superTypeOf CTPath shouldBe true
    CTAny superTypeOf CTList(CTAny) shouldBe true
    CTAny superTypeOf CTVoid shouldBe true

    CTList(CTNumber) superTypeOf CTList(CTInteger) shouldBe true

    CTVoid subTypeOf CTInteger shouldBe true
    CTVoid subTypeOf CTFloat shouldBe true
    CTVoid subTypeOf CTNumber shouldBe true
    CTVoid subTypeOf CTBoolean shouldBe true
    CTVoid subTypeOf CTMap(Map()) shouldBe true
    CTVoid subTypeOf CTNode shouldBe true
    CTVoid subTypeOf CTRelationship shouldBe true
    CTVoid subTypeOf CTPath shouldBe true
    CTVoid subTypeOf CTList(CTAny) shouldBe true
    CTVoid subTypeOf CTVoid shouldBe true
    CTVoid subTypeOf CTList(CTInteger) shouldBe true

    CTBoolean.nullable superTypeOf CTAny shouldBe false
    CTAny superTypeOf CTBoolean.nullable shouldBe false

    CTNumber superTypeOf CTBigDecimal(1, 1) shouldBe true
    CTBigDecimal(1, 1) superTypeOf CTBigDecimal(1, 1) shouldBe true
    CTBigDecimal(2, 1) superTypeOf CTBigDecimal(1, 1) shouldBe true
    CTBigDecimal(2, 2) superTypeOf CTBigDecimal(1, 1) shouldBe true

    CTBigDecimal(2, 1) superTypeOf CTBigDecimal(2, 2) shouldBe false
    CTBigDecimal(2, 2) superTypeOf CTBigDecimal(3, 2) shouldBe false

    CTBigDecimal(2, 2) superTypeOf CTBigDecimal(2, 1) shouldBe false
  }

  it("join") {
    CTInteger join CTFloat shouldBe CTNumber
    CTFloat join CTInteger shouldBe CTNumber
    CTNumber join CTFloat shouldBe CTNumber
    CTNumber join CTInteger shouldBe CTNumber
    CTNumber join CTString shouldBe CTAny

    CTString join CTBoolean shouldBe CTAny
    CTAny join CTInteger shouldBe CTAny

    CTList(CTInteger) join CTList(CTFloat) shouldBe CTList(CTNumber)
    CTList(CTInteger) join CTNode shouldBe CTAny

    CTAny join CTVoid shouldBe CTAny
    CTVoid join CTAny shouldBe CTAny

    CTNode("Car") join CTNode shouldBe CTNode
    CTNode join CTNode("Person") shouldBe CTNode

    CTNumber join CTBigDecimal(1, 1) shouldBe CTNumber
    CTBigDecimal(1, 1) join CTBigDecimal(1, 1) shouldBe CTBigDecimal(1, 1)
    CTBigDecimal(2, 1) join CTBigDecimal(1, 1) shouldBe CTBigDecimal(2, 1)
    CTBigDecimal(1, 1) join CTBigDecimal(2, 2) shouldBe CTBigDecimal(2, 2)

    CTBigDecimal(2, 1) join CTBigDecimal(2, 2) shouldBe CTBigDecimal(3, 2)
    CTBigDecimal(2, 2) join CTBigDecimal(3, 2) shouldBe CTBigDecimal(3, 2)

    CTBigDecimal(2, 2) join CTBigDecimal(2, 1) shouldBe CTBigDecimal(3, 2)

    CTBigDecimal(10, 2) join CTBigDecimal(4, 3) shouldBe CTBigDecimal(11, 3)
  }

  it("join with nullables") {
    CTInteger join CTFloat.nullable shouldBe CTNumber.nullable
    CTFloat.nullable join CTInteger.nullable shouldBe CTNumber.nullable
    CTNumber.nullable join CTString shouldBe CTAny.nullable

    CTString.nullable join CTBoolean.nullable shouldBe CTAny.nullable
    CTAny join CTInteger.nullable shouldBe CTAny.nullable
  }

  it("join with labels and types") {
    CTNode join CTNode("Person") shouldBe CTNode
    CTNode("Other") join CTNode("Person") shouldBe CTNode
    CTNode("Person") join CTNode("Person") shouldBe CTNode("Person")
    CTNode("L1", "L2", "Lx") join CTNode("L1", "L2", "Ly") shouldBe CTNode("L1", "L2")

    CTRelationship join CTRelationship("KNOWS") shouldBe CTRelationship
    CTRelationship("OTHER") join CTRelationship("KNOWS") shouldBe CTRelationship("KNOWS", "OTHER")
    CTRelationship("KNOWS") join CTRelationship("KNOWS") shouldBe CTRelationship("KNOWS")
    CTRelationship("T1", "T2", "Tx") join CTRelationship("T1", "T2", "Ty") shouldBe CTRelationship(
      "T1",
      "T2",
      "Tx",
      "Ty")
  }

  it("meet") {
    CTInteger meet CTNumber shouldBe CTInteger
    CTAny meet CTNumber shouldBe CTNumber

    CTList(CTInteger) meet CTList(CTFloat) shouldBe CTList(CTVoid)
    CTList(CTInteger) meet CTNode shouldBe CTVoid

    CTVoid meet CTInteger shouldBe CTVoid
    CTVoid meet CTAny shouldBe CTVoid

    CTInteger meet CTVoid shouldBe CTVoid

    CTNode meet CTNode("Person") shouldBe CTNode("Person")
  }

  it("meet with labels and types") {
    CTNode("Person") meet CTNode shouldBe CTNode("Person")
    CTNode("Person") meet CTNode("Foo") shouldBe CTNode("Person", "Foo")
    CTNode("Person", "Foo") meet CTNode("Foo") shouldBe CTNode("Person", "Foo")

    CTRelationship("KNOWS") meet CTRelationship shouldBe CTRelationship("KNOWS")
    CTRelationship("KNOWS") meet CTRelationship("LOVES") shouldBe CTVoid
    CTRelationship("KNOWS", "LOVES") meet CTRelationship("LOVES") shouldBe CTRelationship("LOVES")
  }

  it("type equality for all types") {
    for {
      t1 <- allTypes
      t2 <- allTypes
      if t1 == t2
    } yield {
      (t1 subTypeOf t2) shouldBe true
      (t2 subTypeOf t1) shouldBe true
      (t1 superTypeOf t2) shouldBe true
      (t2 superTypeOf t1) shouldBe true
    }
  }

  it("antisymmetry of subtyping") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 subTypeOf t2) (t2 subTypeOf t1) shouldBe (t2 == t1)
        if (t1 superTypeOf t2) (t2 superTypeOf t1) shouldBe (t2 == t1)
      }
    }
  }

  it("type equality between the same type") {
    allTypes.foreach(t => t == t)
    allTypes.foreach(t => t superTypeOf t)
    allTypes.foreach(t => t subTypeOf t)
    allTypes.foreach(t => (t join t) == t)
    allTypes.foreach(t => (t meet t) == t)
  }

  it("contains nullable") {
    CTNode.containsNullable shouldBe false
    CTNode.nullable.containsNullable shouldBe true
    CTList(CTAny).containsNullable shouldBe false
  }

  it("as nullable as") {
    materialTypes.foreach { t =>
      materialTypes.foreach { m =>
        m.asNullableAs(t) should equal(m)
      }
      nullableTypes.foreach { n =>
        n.asNullableAs(t) should equal(n.material)
      }
    }

    nullableTypes.foreach { t =>
      materialTypes.foreach { m =>
        m.asNullableAs(t) should equal(m.nullable)
      }
      nullableTypes.foreach { n =>
        n.asNullableAs(t) should equal(n)
      }
    }
  }

  describe("fromName") {
    it("can parse CypherType names into CypherTypes") {
      allTypes.foreach { t =>
        parseCypherType(t.name) should equal(Some(t))
      }
    }

    it("can parse maps with escaped keys") {
      val input = "MAP(`foo bar_my baz`: STRING)"
      parseCypherType(input) should equal(Some(CTMap(Map("foo bar_my baz" -> CTString))))
    }

    it("can parse node types with escaped labels") {
      val input = "Node(:`foo bar_my baz`:bar)"
      parseCypherType(input) should equal(Some(CTNode("foo bar_my baz", "bar")))
    }

    it("can parse relationship types with escaped labels") {
      val input = "Relationship(:`foo bar_my baz`|:bar)"
      parseCypherType(input) should equal(Some(CTRelationship("foo bar_my baz", "bar")))
    }

    it("handles white space") {
      val input =
        """| Node  (
           |        :`foo bar_my baz` :bar)""".stripMargin
      parseCypherType(input) should equal(Some(CTNode("foo bar_my baz", "bar")))
    }
  }
}
