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
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.api.value.GenCypherValue.any
import org.opencypher.okapi.impl.types.CypherTypeParser.parseCypherType
import org.scalacheck.Prop
import org.scalatestplus.scalacheck.Checkers

import scala.language.postfixOps

class CypherTypesTest extends ApiBaseTest with Checkers {

  val someGraphA = Some(QualifiedGraphName("ns.a"))
  val someGraphB = Some(QualifiedGraphName("ns.b"))

  val materialTypes: Seq[CypherType] = Seq(
    CTAnyMaterial,
    CTTrue,
    CTFalse,
    CTBoolean,
    CTNumber,
    CTInteger,
    CTFloat,
    CTBigDecimal(12),
    CTString,
    CTMap,
    CTMap(Map("foo" -> CTString, "bar" -> CTInteger)),
    CTNode.empty(),
    CTNode.empty("Person"),
    CTNode.empty("Person", "Employee"),
    CTNode.fromCombo(Set("Person"), Map.empty[String, CypherType], someGraphA),
    CTNode.fromCombo(Set("Person", "Employee"), Map.empty[String, CypherType], someGraphA),
    CTNode.fromCombo(Set("Person"), Map.empty[String, CypherType], someGraphB),
    CTNode.fromCombo(Set("Person", "Employee"), Map.empty[String, CypherType], someGraphB),
    CTRelationship.empty("KNOWS"),
    CTRelationship.empty("KNOWS", "LOVES"),
    CTRelationship(AnyOf("KNOWS"), Map.empty[String, CypherType], someGraphA),
    CTRelationship(AnyOf("KNOWS", "LOVES"),Map.empty[String, CypherType],  someGraphA),
    CTRelationship(AnyOf("KNOWS"), Map.empty[String, CypherType],  someGraphB),
    CTRelationship(AnyOf("KNOWS", "LOVES"),Map.empty[String, CypherType],  someGraphB),
    CTPath,
    CTList(CTAnyMaterial),
    CTList(CTAny),
    CTList(CTList(CTBoolean)),
    CTList(CTString.nullable),
    CTVoid
  )

  val nullableTypes: Seq[CypherType] =
    materialTypes.map(_.nullable)

  val allTypes: Seq[CypherType] =
    materialTypes ++ nullableTypes

  it("couldBe") {
    CTAnyMaterial couldBeSameTypeAs CTNode.empty() shouldBe true
    CTNode.empty() couldBeSameTypeAs CTAnyMaterial shouldBe true
    CTInteger couldBeSameTypeAs CTNumber shouldBe true
    CTNumber couldBeSameTypeAs CTInteger shouldBe true
    CTFloat couldBeSameTypeAs CTInteger shouldBe false
    CTBoolean couldBeSameTypeAs CTInteger shouldBe false

    CTList(CTInteger) couldBeSameTypeAs CTList(CTFloat) shouldBe false
    CTList(CTInteger) couldBeSameTypeAs CTList(CTAnyMaterial) shouldBe true
    CTList(CTAnyMaterial) couldBeSameTypeAs CTList(CTInteger) shouldBe true

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

//  it("intersects") {
//    CTAnyMaterial intersects CTNode.empty() shouldBe true
//    CTNode intersects CTAnyMaterial shouldBe true
//    CTInteger intersects CTNumber shouldBe true
//    CTNumber intersects CTInteger shouldBe true
//    CTFloat intersects CTInteger shouldBe false
//    CTBoolean intersects CTInteger shouldBe false
//
//    CTRelationship intersects CTNode shouldBe false
//
//    CTList(CTInteger) intersects CTList(CTFloat) shouldBe true
//    CTList(CTInteger) intersects CTList(CTAnyMaterial) shouldBe true
//    CTList(CTAnyMaterial) intersects CTList(CTInteger) shouldBe true
//
//    CTNull intersects CTInteger.nullable shouldBe true
//    CTInteger.nullable intersects CTNull shouldBe true
//
//    CTString.nullable intersects CTInteger.nullable shouldBe true
//    CTNode.nullable intersects CTBoolean.nullable shouldBe true
//
//    CTVoid intersects CTBoolean shouldBe false
//    CTVoid intersects CTBoolean.nullable shouldBe false
//    CTVoid intersects CTAnyMaterial shouldBe false
//    CTVoid intersects CTBoolean.nullable shouldBe false
//  }

  it("joining with list of void") {
    val otherList = CTList(CTString).nullable

    CTEmptyList join otherList should equal(otherList)
    otherList join CTEmptyList should equal(otherList)
  }

  it("type names") {
    Seq[(CypherType, (String, String))](
      CTAnyMaterial -> ("ANY" -> "ANY?"),
      CTString -> ("STRING" -> "STRING?"),
      CTBoolean -> ("BOOLEAN" -> "BOOLEAN?"),
      CTNumber -> ("NUMBER" -> "NUMBER?"),
      CTInteger -> ("INTEGER" -> "INTEGER?"),
      CTFloat -> ("FLOAT" -> "FLOAT?"),
      CTMap(Map("foo" -> CTString, "bar" -> CTInteger)) -> ("MAP(foo: STRING, bar: INTEGER)" -> "MAP(foo: STRING, bar: INTEGER)?"),
//      CTNode.empty() -> ("NODE" -> "NODE?"),
//      CTNode.empty("Person") -> ("NODE(:Person)" -> "NODE(:Person)?"),
//      CTNode.empty("Person", "Employee") -> ("NODE(:Person:Employee)" -> "NODE(:Person:Employee)?"),
//      CTNode(AnyOf("Person"), Map.empty[String, CypherType], Some(QualifiedGraphName("foo.bar"))) -> ("NODE(:Person) @ foo.bar" -> "NODE(:Person) @ foo.bar?"),
//      CTRelationship.empty() -> ("RELATIONSHIP" -> "RELATIONSHIP?"),
//      CTRelationship.empty("KNOWS") -> ("RELATIONSHIP(:KNOWS)" -> "RELATIONSHIP(:KNOWS)?"),
//      CTRelationship.empty("KNOWS", "LOVES") -> ("RELATIONSHIP(:KNOWS|:LOVES)" -> "RELATIONSHIP(:KNOWS|:LOVES)?"),
//      CTRelationship(AnyOf("KNOWS"),Map.empty[String, CypherType], Some(QualifiedGraphName("foo.bar"))) -> ("RELATIONSHIP(:KNOWS) @ foo.bar" -> "RELATIONSHIP(:KNOWS) @ foo.bar?"),
      CTPath -> ("PATH" -> "PATH?"),
      CTList(CTInteger) -> ("LIST(INTEGER)" -> "LIST(INTEGER)?"),
      CTList(CTInteger.nullable) -> ("LIST(INTEGER?)" -> "LIST(INTEGER?)?"),
      CTDate -> ("DATE" -> "DATE?"),
      CTLocalDateTime -> ("LOCALDATETIME" -> "LOCALDATETIME?")
    ).foreach {
      case (t, (materialName, nullableName)) =>
        t.isNullable shouldBe false
        t.name shouldBe materialName
        t.nullable.name shouldBe nullableName
    }

    CTVoid.name shouldBe "VOID"
    CTNull.name shouldBe "NULL"
  }

  // todo add tests with properties
  it("RELATIONSHIP type") {
    CTRelationship.empty("KNOWS").superTypeOf(CTRelationship.empty("KNOWS")) shouldBe true
    CTRelationship.empty("KNOWS").superTypeOf(CTRelationship.empty("KNOWS", "LOVES")) shouldBe false
    CTRelationship.empty("KNOWS", "LOVES").superTypeOf(CTRelationship.empty("LOVES")) shouldBe true
    CTRelationship.empty("KNOWS").superTypeOf(CTRelationship.empty("NOSE")) shouldBe false
  }

  // todo add tests with properties
  it("RELATIONSHIP? type") {
    CTRelationship.empty("KNOWS").nullable.superTypeOf(CTRelationship.empty("KNOWS").nullable) shouldBe true
    CTRelationship.empty("KNOWS").nullable.superTypeOf(CTRelationship.empty("KNOWS", "LOVES").nullable) shouldBe false
    CTRelationship.empty("KNOWS", "LOVES").nullable.superTypeOf(CTRelationship.empty("LOVES").nullable) shouldBe true
    CTRelationship.empty("KNOWS").nullable.superTypeOf(CTRelationship.empty("NOSE").nullable) shouldBe false
    CTRelationship.empty("FOO").nullable.superTypeOf(CTNull) shouldBe true
  }

  it("NODE type") {
    CTNode.empty().superTypeOf(CTNode.empty()) shouldBe true
    CTNode.empty().superTypeOf(CTNode.empty("Person")) shouldBe false
    CTNode.empty("Person").superTypeOf(CTNode.empty()) shouldBe false
    CTNode.empty().subTypeOf(CTNode.empty("Person")) shouldBe false
    CTNode.empty("Person").superTypeOf(CTNode.empty("Person")) shouldBe true
    CTNode.empty("Person").superTypeOf(CTNode.empty("Person", "Employee")) shouldBe true
    CTNode.empty("Person", "Employee").superTypeOf(CTNode.empty("Employee")) shouldBe false
    CTNode.empty("Person").superTypeOf(CTNode.empty("Foo")) shouldBe false
    CTNode.empty("Person").superTypeOf(CTNode.empty()) shouldBe false
  }

  it("NODE? type") {
    CTNode.empty().nullable.superTypeOf(CTNode.empty().nullable) shouldBe true
    CTNode.empty().nullable.superTypeOf(CTNode.empty("Person").nullable) shouldBe false
    CTNode.empty("Person").nullable.superTypeOf(CTNode.empty("Person").nullable) shouldBe true
    CTNode.empty("Person").nullable.superTypeOf(CTNode.empty("Person", "Employee").nullable) shouldBe true
    CTNode.empty("Person", "Employee").nullable.superTypeOf(CTNode.empty("Employee").nullable) shouldBe false
    CTNode.empty("Person").nullable.superTypeOf(CTNode.empty("Foo").nullable) shouldBe false
    CTNode.empty("Foo").nullable.superTypeOf(CTNull) shouldBe true
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

    CTAnyMaterial superTypeOf CTInteger shouldBe true
    CTAnyMaterial superTypeOf CTFloat shouldBe true
    CTAnyMaterial superTypeOf CTNumber shouldBe true
    CTAnyMaterial superTypeOf CTBoolean shouldBe true
    CTAnyMaterial superTypeOf CTMap() shouldBe true
    CTAnyMaterial superTypeOf CTNode.empty() shouldBe true
    CTAnyMaterial superTypeOf CTPath shouldBe true
    CTAnyMaterial superTypeOf CTList(CTAnyMaterial) shouldBe true
    CTAnyMaterial superTypeOf CTVoid shouldBe true

    CTList(CTNumber) superTypeOf CTList(CTInteger) shouldBe true

    CTVoid subTypeOf CTInteger shouldBe true
    CTVoid subTypeOf CTFloat shouldBe true
    CTVoid subTypeOf CTNumber shouldBe true
    CTVoid subTypeOf CTBoolean shouldBe true
    CTVoid subTypeOf CTMap() shouldBe true
    CTVoid subTypeOf CTNode.empty() shouldBe true
    CTVoid subTypeOf CTPath shouldBe true
    CTVoid subTypeOf CTList(CTAnyMaterial) shouldBe true
    CTVoid subTypeOf CTVoid shouldBe true
    CTVoid subTypeOf CTList(CTInteger) shouldBe true

    CTBoolean.nullable superTypeOf CTAnyMaterial shouldBe false
    CTAnyMaterial superTypeOf CTBoolean.nullable shouldBe false

    CTNumber superTypeOf CTBigDecimal(1, 1) shouldBe true
    CTBigDecimal(1, 1) superTypeOf CTBigDecimal(1, 1) shouldBe true
    CTBigDecimal(2, 1) superTypeOf CTBigDecimal(1, 1) shouldBe true
    CTBigDecimal(2, 2) superTypeOf CTBigDecimal(1, 1) shouldBe true

    CTBigDecimal(2, 1) superTypeOf CTBigDecimal(2, 2) shouldBe false
    CTBigDecimal(2, 2) superTypeOf CTBigDecimal(3, 2) shouldBe false

    CTBigDecimal(2, 2) superTypeOf CTBigDecimal(2, 1) shouldBe false

    CTBigDecimal superTypeOf CTBigDecimal(2, 1) shouldBe true
    CTBigDecimal(2, 1) superTypeOf CTBigDecimal shouldBe false
  }

  it("join") {
    (CTInteger join CTFloat).subTypeOf(CTNumber) shouldBe true
    (CTFloat join CTInteger).subTypeOf(CTNumber) shouldBe true
    CTNumber join CTFloat shouldBe CTNumber
    CTNumber join CTInteger shouldBe CTNumber
    CTNumber join CTBigDecimal shouldBe CTNumber
    CTNumber join CTString shouldBe CTUnion(CTString, CTInteger, CTFloat, CTBigDecimal)

    CTString join CTBoolean shouldBe CTUnion(CTString, CTBoolean)
    CTAnyMaterial join CTInteger shouldBe CTAnyMaterial

    CTList(CTInteger) join CTList(CTFloat) shouldBe CTUnion(CTList(CTInteger), CTList(CTFloat))
    CTList(CTInteger) join CTNode.empty() shouldBe CTUnion(CTList(CTInteger), CTNode.empty())

    CTAnyMaterial join CTVoid shouldBe CTAnyMaterial
    CTVoid join CTAnyMaterial shouldBe CTAnyMaterial

    val rhsNodeType = CTNode(AnyOf.alternatives(Set("Car", "Animal")), Map.empty[String, CypherType], None)
    CTNode.empty("Car") join rhsNodeType shouldBe rhsNodeType
    CTNode.empty() join CTNode.empty("Person") shouldBe CTNode(AnyOf(Set(AllOf("Person"), AllOf.empty)), Map.empty[String, CypherType])

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
    CTInteger join CTFloat.nullable join CTBigDecimal shouldBe CTNumber.nullable
    CTFloat.nullable join CTInteger.nullable join CTBigDecimal shouldBe CTNumber.nullable
    CTNumber.nullable join CTString shouldBe CTUnion(CTString, CTFloat, CTInteger, CTBigDecimal, CTNull)

    CTString.nullable join CTBoolean.nullable shouldBe CTUnion(CTString, CTNull, CTTrue, CTFalse)
    CTAnyMaterial join CTInteger.nullable shouldBe CTAnyMaterial.nullable
  }

  it("join with labels and types") {
    CTNode.empty() join CTNode.empty("Person") shouldBe CTNode(AnyOf(Set(AllOf(Set.empty[String]),   AllOf(Set("Person")))), Map.empty[String, CypherType])
    CTNode.empty("Other") join CTNode.empty("Person") shouldBe CTNode(AnyOf.alternatives(Set("Person", "Other")), Map.empty[String, CypherType])
    CTNode.empty("Person") join CTNode.empty("Person") shouldBe CTNode.empty("Person")
    CTNode.empty("L1", "L2", "Lx") join CTNode.empty("L1", "L2", "Ly") shouldBe CTNode(
      AnyOf(
        Set(
          AllOf("L1", "L2", "Lx"),
          AllOf("L1", "L2", "Ly")
        )
      ),
      Map.empty[String, CypherType]
    )

    CTRelationship.empty("OTHER") join CTRelationship.empty("KNOWS") shouldBe CTRelationship.empty("KNOWS", "OTHER")
    CTRelationship.empty("KNOWS") join CTRelationship.empty("KNOWS") shouldBe CTRelationship.empty("KNOWS")
    CTRelationship.empty("T1", "T2", "Tx") join CTRelationship.empty("T1", "T2", "Ty") shouldBe CTRelationship.empty("T1", "T2", "Tx", "Ty")
  }

//  it("meet") {
//    CTInteger meet CTNumber shouldBe CTInteger
//    CTAnyMaterial meet CTNumber shouldBe CTNumber
//
//    CTList(CTInteger) meet CTList(CTFloat) shouldBe CTEmptyList
//    CTList(CTInteger) meet CTNode shouldBe CTVoid
//
//    CTVoid meet CTInteger shouldBe CTVoid
//    CTVoid meet CTAnyMaterial shouldBe CTVoid
//
//    CTInteger meet CTVoid shouldBe CTVoid
//
//    CTNode meet CTNode("Person") shouldBe CTNode("Person")
//
//    CTMap("age" -> CTInteger) meet CTMap() shouldBe CTMap("age" -> CTInteger.nullable)
//    CTMap() meet CTMap("age" -> CTInteger)  shouldBe CTMap("age" -> CTInteger.nullable)
//    CTMap("age" -> CTInteger) meet CTMap shouldBe CTMap("age" -> CTInteger)
//  }

//  it("meet with labels and types") {
//    CTNode.empty("Person") meet CTNode.empty() shouldBe CTNode.empty("Person")
//    CTNode.empty("Person") meet CTNode.empty("Foo") shouldBe CTNode.empty("Person", "Foo")
//    CTNode.empty("Person", "Foo") meet CTNode.empty("Foo") shouldBe CTNode.empty("Person", "Foo")
//
//    CTRelationship.empty("KNOWS") meet CTRelationship shouldBe CTRelationship.empty("KNOWS")
//    CTRelationship.empty("KNOWS") meet CTRelationship.empty("LOVES") shouldBe CTVoid
//    CTRelationship.empty("KNOWS", "LOVES") meet CTRelationship.empty("LOVES") shouldBe CTRelationship.empty("LOVES")
//  }

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
//    allTypes.foreach(t => (t meet t) == t)
  }

  // TODO: do we need that still?
//  it("contains nullable") {
//    CTNode.containsNullable shouldBe false
//    CTNode.nullable.containsNullable shouldBe true
//    CTList(CTAnyMaterial).containsNullable shouldBe false
//  }

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

    // TODO enable parsing of CTNode and CTRelationship
    it("can parse CypherType names into CypherTypes") {
      allTypes.filterNot {
        case _: CTElement => true
        case u: CTUnion => u.alternatives.exists(_.isInstanceOf[CTElement])
        case _ => false
      }.foreach { t =>
        parseCypherType(t.name) should equal(Some(t))
      }
    }

    it("can parse maps with escaped keys") {
      val input = "MAP(`foo bar_my baz`: STRING)"
      parseCypherType(input) should equal(Some(CTMap(Map("foo bar_my baz" -> CTString))))
    }

    // TODO enable parsing of CTNode and CTRelationship
    //    it("can parse node types with escaped labels") {
//      val input = "Node(:`foo bar_my baz`:bar)"
//      parseCypherType(input) should equal(Some(CTNode.empty("foo bar_my baz", "bar")))
//    }
//
//    it("can parse relationship types with escaped labels") {
//      val input = "Relationship(:`foo bar_my baz`|:bar)"
//      parseCypherType(input) should equal(Some(CTRelationship.empty("foo bar_my baz", "bar")))
//    }
//
//    it("handles white space") {
//      val input =
//        """| Node  (
//           |        :`foo bar_my baz` :bar)""".stripMargin
//      parseCypherType(input) should equal(Some(CTNode.empty("foo bar_my baz", "bar")))
//    }
  }

  it("types for literals") {
    check(Prop.forAll(any) { v: CypherValue =>
      (v.cypherType | CTNull).isNullable === true
    }, minSuccessful(100))

    check(Prop.forAll(any) { v: CypherValue =>
      (v.cypherType | CTNull).material.isNullable === false
    }, minSuccessful(100))

    check(Prop.forAll(any) { v: CypherValue =>
      v.cypherType.nullable.isNullable === true
    }, minSuccessful(100))

    check(Prop.forAll(any) { v: CypherValue =>
      v.cypherType.nullable.material.isNullable === false
    }, minSuccessful(100))

    check(Prop.forAll(any) { v: CypherValue =>
      (v.cypherType | CTNull) === v.cypherType.nullable
    }, minSuccessful(100))

    check(Prop.forAll(any) { v: CypherValue =>
      CTUnion(v.cypherType) === v.cypherType
    }, minSuccessful(100))

    check(Prop.forAll(any) { v: CypherValue =>
      CTUnion(v.cypherType, CTNull) === v.cypherType.nullable
    }, minSuccessful(100))
  }

//  it("intersects map types with different properties") {
//    CTMap(Map("name" -> CTString)) & CTMap(Map("age" -> CTInteger)) should equal(
//      CTMap(Map("name" -> CTString.nullable, "age" -> CTInteger.nullable))
//    )
//  }
//
//  it("intersects map types with overlapping properties") {
//    CTMap(Map("name" -> CTString)) & CTMap(Map("name" -> CTInteger)) should equal(
//      CTMap(Map("name" -> (CTString | CTInteger)))
//    )
//  }

}
