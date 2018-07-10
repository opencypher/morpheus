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
package org.opencypher.okapi.api.types

import org.opencypher.okapi.api.types.CypherType._
import org.scalatest.{FunSpec, Matchers}

import scala.language.postfixOps

class CypherTypesTest extends FunSpec with Matchers {

  val materialTypes: Seq[CypherType] = Seq(
    CTAny,
    CTBoolean,
    CTNumber,
    CTInteger,
    CTFloat,
    CTString,
    //    CTMap,
    CTAnyNode,
    CTNode("Person"),
    CTNode("Person", "Employee"),
    CTAnyRelationship,
    CTRelationship("KNOWS"),
    CTRelationship("KNOWS", "LOVES"),
    //    CTPath,
    CTList(CTAny),
    CTList(CTList(CTBoolean)),
    CTList(CTAny),
    CTList(CTString.nullable),
    CTAny,
    CypherType.CTVoid
  )

  val nullableTypes: Seq[CypherType] =
    materialTypes.map(_.nullable)

  val allTypes: Seq[CypherType] =
    materialTypes ++ nullableTypes

  //  it("couldBe") {
  //    CTAny couldBeSameTypeAs CTNode shouldBe true
  //    CTNode couldBeSameTypeAs CTAny shouldBe true
  //    CTInteger couldBeSameTypeAs CTNumber shouldBe true
  //    CTNumber couldBeSameTypeAs CTInteger shouldBe true
  //    CTFloat couldBeSameTypeAs CTInteger shouldBe false
  //    CTBoolean couldBeSameTypeAs CTInteger shouldBe false
  //
  //    CTNode couldBeSameTypeAs CTMap shouldBe true
  //    CTRelationship couldBeSameTypeAs CTNode shouldBe false
  //    CTRelationship couldBeSameTypeAs CTMap shouldBe true
  //
  //    CTList(CTInteger) couldBeSameTypeAs CTList(CTFloat) shouldBe false
  //    CTList(CTInteger) couldBeSameTypeAs CTList(CTAny) shouldBe true
  //    CTList(CTAny) couldBeSameTypeAs CTList(CTInteger) shouldBe true
  //  }

  it("union with list of void") {
    val voidList = CTList(CTVoid)
    val otherList = CTList(CTString).nullable

    voidList union otherList should equal(otherList)
    otherList union voidList should equal(otherList)
  }

  it("type names") {
    CTNode("Person", "Employee").legacyName

    Seq[(CypherType, (String, String))](
      CTString -> ("STRING" -> "STRING?"),
      CTBoolean -> ("BOOLEAN" -> "BOOLEAN?"),
      CTNumber -> ("NUMBER" -> "NUMBER?"),
      CTInteger -> ("INTEGER" -> "INTEGER?"),
      CTFloat -> ("FLOAT" -> "FLOAT?"),
      CTAnyMap -> ("MAP" -> "MAP?"),
      CTAnyNode -> ("NODE" -> "NODE?"),
      CTNode("Person") -> ("NODE(:Person)" -> "NODE(:Person)?"),
      CTNode("Person", "Employee") -> ("NODE(:Person:Employee)" -> "NODE(:Person:Employee)?"),
//      CTNode(Set("Person"), Some(QualifiedGraphName("foo.bar"))) -> ("NODE(:Person) @ foo.bar" -> "NODE(:Person) @ foo.bar?"),
      CTAnyRelationship -> ("RELATIONSHIP" -> "RELATIONSHIP?"),
      CTRelationship(Set("KNOWS")) -> ("RELATIONSHIP(:KNOWS)" -> "RELATIONSHIP(:KNOWS)?"),
      CTRelationship(Set("KNOWS", "LOVES")) -> ("RELATIONSHIP(:KNOWS|LOVES)" -> "RELATIONSHIP(:KNOWS|LOVES)?"),
//      CTRelationship(Set("KNOWS"), Some(QualifiedGraphName("foo.bar"))) -> ("RELATIONSHIP(:KNOWS) @ foo.bar" -> "RELATIONSHIP(:KNOWS) @ foo.bar?"),
      CTPath -> ("PATH" -> "PATH?"),
      CTList(CTInteger) -> ("LIST OF INTEGER" -> "LIST? OF INTEGER"),
      CTList(CTInteger.nullable) -> ("LIST OF INTEGER?" -> "LIST? OF INTEGER?")
//      CTWildcard -> ("?" -> "??")
    ).foreach {
      case (t, (materialName, nullableName)) =>
        println(t)
        t.isNullable shouldBe false
        t.legacyName shouldBe materialName
        t.nullable.legacyName shouldBe nullableName
    }

    CTAny.legacyName shouldBe "ANY"
    CTAny.nullable.legacyName shouldBe "ANY"
    CTVoid.legacyName shouldBe "VOID"
    CTNull.legacyName shouldBe "NULL"
    CTNull.nullable.legacyName shouldBe "NULL"
  }

  it("can parse CypherType names into CypherTypes") {
    allTypes.foreach { t =>
      println(t)
      CypherType.fromLegacyName(t.legacyName).get should equal(t)
    }
  }

  it("RELATIONSHIP type") {
    CTAnyRelationship.superTypeOf(CTAnyRelationship) shouldBe true
    CTAnyRelationship.superTypeOf(CTRelationship("KNOWS")) shouldBe true
    CTRelationship("KNOWS").superTypeOf(CTAnyRelationship) shouldBe false
    CTAnyRelationship.subTypeOf(CTRelationship("KNOWS")) shouldBe false
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS")) shouldBe true
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS", "LOVES")) shouldBe false
    CTRelationship("KNOWS", "LOVES").superTypeOf(CTRelationship("LOVES")) shouldBe true
    CTRelationship("KNOWS").superTypeOf(CTRelationship("NOSE")) shouldBe false
  }

  it("RELATIONSHIP? type") {
    CTAnyRelationship.nullable.superTypeOf(CTAnyRelationship.nullable) shouldBe true
    CTAnyRelationship.nullable.superTypeOf(CTRelationship("KNOWS").nullable) shouldBe true
    CTRelationship("KNOWS").nullable.superTypeOf(CTRelationship("KNOWS").nullable) shouldBe true
    CTRelationship("KNOWS").nullable.superTypeOf(CTRelationship("KNOWS", "LOVES").nullable) shouldBe false
    CTRelationship("KNOWS", "LOVES").nullable.superTypeOf(CTRelationship("LOVES").nullable) shouldBe true
    CTRelationship("KNOWS").nullable.superTypeOf(CTRelationship("NOSE").nullable) shouldBe false
    CTRelationship("FOO").nullable.superTypeOf(CTNull) shouldBe true
  }

  it("NODE type") {
    CTAnyNode.superTypeOf(CTAnyNode) shouldBe true
    CTAnyNode.superTypeOf(CTNode("Person")) shouldBe true
    CTNode("Person").superTypeOf(CTAnyNode) shouldBe false
    CTAnyNode.subTypeOf(CTNode("Person")) shouldBe false
    CTNode("Person").superTypeOf(CTNode("Person")) shouldBe true
    CTNode("Person").superTypeOf(CTNode("Person", "Employee")) shouldBe true
    CTNode("Person", "Employee").superTypeOf(CTNode("Employee")) shouldBe false
    CTNode("Person").superTypeOf(CTNode("Foo")) shouldBe false
    CTNode("Person").superTypeOf(CTAnyNode) shouldBe false
  }

  it("NODE? type") {
    CTAnyNode.nullable.superTypeOf(CTAnyNode.nullable) shouldBe true
    CTAnyNode.nullable.superTypeOf(CTNode("Person")) shouldBe true
    CTNode("Person").nullable.superTypeOf(CTNode("Person").nullable) shouldBe true
    CTNode("Person").union(CTNode("Employee"))
    CTNode("Person").nullable.superTypeOf(CTNode("Person", "Employee").nullable) shouldBe true
    CTNode("Person", "Employee").nullable.superTypeOf(CTNode("Employee").nullable) shouldBe false
    CTNode("Person").nullable.superTypeOf(CTNode("Foo").nullable) shouldBe false
    CTNode("Foo").nullable.superTypeOf(CTNull) shouldBe true

    println(CTNode("Person", "Employee").nullable)
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
    CTAnyMap superTypeOf CTAnyMap shouldBe true
    //    CTMap superTypeOf CTNode shouldBe true
    //    CTMap superTypeOf CTRelationship shouldBe true

    CTAny superTypeOf CTInteger shouldBe true
    CTAny superTypeOf CTFloat shouldBe true
    CTAny superTypeOf CTNumber shouldBe true
    CTAny superTypeOf CTBoolean shouldBe true
    CTAny superTypeOf CTAnyMap shouldBe true
    CTAny superTypeOf CTAnyNode shouldBe true
    CTAny superTypeOf CTAnyRelationship shouldBe true
    CTAny superTypeOf CTPath shouldBe true
    CTAny superTypeOf CTList(CTAny) shouldBe true
    CTAny superTypeOf CTVoid shouldBe true

    CTList(CTNumber) superTypeOf CTList(CTInteger) shouldBe true

    CTVoid subTypeOf CTInteger shouldBe true
    CTVoid subTypeOf CTFloat shouldBe true
    CTVoid subTypeOf CTNumber shouldBe true
    CTVoid subTypeOf CTBoolean shouldBe true
    CTVoid subTypeOf CTAnyMap shouldBe true
    CTVoid subTypeOf CTAnyNode shouldBe true
    CTVoid subTypeOf CTAnyRelationship shouldBe true
    CTVoid subTypeOf CTPath shouldBe true
    CTVoid subTypeOf CTList(CTAny) shouldBe true
    CTVoid subTypeOf CTVoid shouldBe true
    CTVoid subTypeOf CTList(CTInteger) shouldBe true

    CTBoolean.nullable superTypeOf CTAny shouldBe false
    // TODO: Disagree
    //    CTAny superTypeOf CTBoolean.nullable shouldBe false

    CTAnyNode.nullable.isInstanceOf[CTNode] should be(true)
  }

  it("union") {
    CTInteger union CTFloat shouldBe CTNumber
    CTFloat union CTInteger shouldBe CTNumber
    CTNumber union CTFloat shouldBe CTNumber
    CTNumber union CTInteger shouldBe CTNumber
    CTNumber union CTString shouldBe CTUnion(CTFloat, CTInteger, CTString)

    //    CTNode union CTRelationship shouldBe CTMap
    //    CTNode union CTMap shouldBe CTMap
    CTString union CTBoolean shouldBe CTUnion(CTString, CTBoolean)
    CTAny union CTInteger shouldBe CTAny

    CTList(CTInteger union CTFloat) shouldBe CTList(CTNumber)
    CTList(CTInteger) union CTList(CTFloat) shouldBe CTUnion(CTList(CTInteger), CTList(CTFloat))
    CTList(CTInteger) union CTAnyNode shouldBe CTUnion(CTAnyNode, CTList(CTInteger))

    //    CTAny union CTWildcard shouldBe CTAny
    CTAny union CTVoid shouldBe CTAny
    //    CTWildcard union CTAny shouldBe CTAny
    CTVoid union CTAny shouldBe CTAny

    CTNode("Car") union CTAnyNode shouldBe CTAnyNode
    CTAnyNode union CTNode("Person") shouldBe CTAnyNode
  }

  it("union with nullables") {
    CTInteger union CTFloat.nullable shouldBe CTNumber.nullable
    CTFloat.nullable union CTInteger.nullable shouldBe CTNumber.nullable
    CTNumber.nullable union CTString shouldBe CTUnion(CTFloat, CTInteger, CTString, CTNull)

    //    CTNode union CTAnyRelationship.nullable shouldBe CTMap.nullable
    //    CTAnyNode.nullable union CTMap shouldBe CTMap.nullable
    CTString.nullable union CTBoolean.nullable shouldBe CTUnion(CTBoolean, CTString, CTNull)
    CTAny union CTInteger.nullable shouldBe CTAny.nullable
  }

  it("union with labels and types") {
    CTAnyNode union CTNode("Person") shouldBe CTAnyNode
    CTNode("Other") union CTNode("Person") shouldBe CTUnion(CTNode("Other"), CTNode("Person"))
    CTNode("Person") union CTNode("Person") shouldBe CTNode("Person")

    CTNode("L1", "L2", "Lx") union CTNode("L1", "L2", "Ly") shouldBe CTNode("L1", "L2")

    CTAnyRelationship union CTRelationship("KNOWS") shouldBe CTAnyRelationship
    CTRelationship("OTHER") union CTRelationship("KNOWS") shouldBe CTRelationship("KNOWS", "OTHER")
    CTRelationship("KNOWS") union CTRelationship("KNOWS") shouldBe CTRelationship("KNOWS")
    CTRelationship("T1", "T2", "Tx") union CTRelationship("T1", "T2", "Ty") shouldBe CTRelationship(
      "T1",
      "T2",
      "Tx",
      "Ty")

    //    CTNode("Person") union CTRelationship("KNOWS") shouldBe CTMap
    //    CTNode("Person") union CTRelationship shouldBe CTMap
    //    CTRelationship("KNOWS") union CTNode("Person") shouldBe CTMap
    //    CTRelationship("KNOWS") union CTNode shouldBe CTMap
  }

  it("intersect") {
    CTInteger intersect CTNumber shouldBe CTInteger
    CTAny intersect CTNumber shouldBe CTNumber

    CTList(CTInteger) intersect CTList(CTFloat) shouldBe CTList(CTVoid)
    CTList(CTInteger) intersect CTAnyNode shouldBe CTVoid
    // TODO: Disagree
    CTList(CTAny) intersect CTList(CTNumber) shouldBe CTList(CTNumber)

    CTVoid intersect CTInteger shouldBe CTVoid
    CTVoid intersect CTAny shouldBe CTVoid
    CTVoid intersect CTAny shouldBe CTVoid

    CTInteger intersect CTVoid shouldBe CTVoid
    CTAny intersect CTVoid shouldBe CTVoid

    CTAnyNode intersect CTNode("Person") shouldBe CTNode("Person")
  }

  it("intersect with labels and types") {
    //    CTMap intersect CTNode shouldBe CTNode
    //    CTMap intersect CTNode("Person") shouldBe CTNode("Person")
    //    CTMap intersect CTRelationship("KNOWS") shouldBe CTRelationship("KNOWS")

    CTNode("Person") intersect CTAnyNode shouldBe CTNode("Person")
    CTNode("Person") intersect CTNode("Foo") shouldBe CTNode("Person", "Foo")
    CTNode("Person", "Foo") intersect CTNode("Foo") shouldBe CTNode("Person", "Foo")

    CTRelationship("KNOWS") intersect CTAnyRelationship shouldBe CTRelationship("KNOWS")
    CTRelationship("KNOWS") intersect CTRelationship("LOVES") shouldBe CTVoid
    CTRelationship("KNOWS", "LOVES") intersect CTRelationship("LOVES") shouldBe CTRelationship("LOVES")
  }

  //  it("type equality between different types") {
  //    allTypes.foreach { t1 =>
  //      allTypes.foreach { t2 =>
  //        val result = t1 sameTypeAs t2
  //        (result.isDefinite) should be(
  //          (!t1.isWildcard && !t2.isWildcard) ||
  //            (t1.isNullable && !t2.isNullable) ||
  //            (!t1.isNullable && t2.isNullable)
  //        )
  //
  //        result match {
  //          case true =>
  //            (t1 subTypeOf t2).maybetrue shouldBe true
  //            (t2 subTypeOf t1).maybetrue shouldBe true
  //            (t1 superTypeOf t2).maybetrue shouldBe true
  //            (t2 superTypeOf t1).maybetrue shouldBe true
  //
  //          case false =>
  //            if (t1 subTypeOf t2 istrue)
  //              (t2 subTypeOf t1).maybefalse shouldBe true
  //
  //            if (t2 subTypeOf t1 istrue)
  //              (t1 subTypeOf t2).maybefalse shouldBe true
  //
  //            if (t1 superTypeOf t2 istrue)
  //              (t2 superTypeOf t1).maybefalse shouldBe true
  //
  //            if (t2 superTypeOf t1 istrue)
  //              (t1 superTypeOf t2).maybefalse shouldBe true
  //
  //          case Maybe =>
  //            (
  //              (t1.isWildcard || t2.isWildcard) ||
  //                (t1.isNullable && !t2.isNullable) ||
  //                (!t1.isNullable && t2.isNullable)
  //            ) shouldBe true
  //        }
  //      }
  //    }
  //  }

  it("antisymmetry of subtyping") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 subTypeOf t2) (t2 subTypeOf t1) shouldBe (t2 == t1)
        if (t1 superTypeOf t2) (t2 superTypeOf t1) shouldBe (t2 == t1)
      }
    }
  }
  //
  //  it("type equality between the same type") {
  //    allTypes.foreach(t => t == t)
  //    allTypes.foreach(t => t superTypeOf t)
  //    allTypes.foreach(t => t subTypeOf t)
  //    allTypes.foreach(t => (t union t) == t)
  //    allTypes.foreach(t => (t intersect t) == t)
  //  }
  //
  //  it("computing definite types (type erasure)") {
  //    CTWildcard.wildcardErasedSuperType sameTypeAs CTAny shouldBe true
  //    CTWildcard.nullable.wildcardErasedSuperType sameTypeAs CTAny.nullable shouldBe true
  //    CTList(CTWildcard).wildcardErasedSuperType sameTypeAs CTList(CTAny) shouldBe true
  //    CTList(CTWildcard.nullable).wildcardErasedSuperType sameTypeAs CTList(CTAny.nullable) shouldBe true
  //    CTList(CTBoolean).wildcardErasedSuperType sameTypeAs CTList(CTBoolean) shouldBe true
  //    CTList(CTBoolean).nullable.wildcardErasedSuperType sameTypeAs CTList(CTBoolean).nullable shouldBe true
  //
  //    CTWildcard.wildcardErasedSubType sameTypeAs CTVoid shouldBe true
  //    CTWildcard.nullable.wildcardErasedSubType sameTypeAs CTNull shouldBe true
  //    CTList(CTWildcard).wildcardErasedSubType sameTypeAs CTList(CTVoid) shouldBe true
  //    CTList(CTWildcard.nullable).wildcardErasedSubType sameTypeAs CTList(CTNull) shouldBe true
  //    CTList(CTBoolean).wildcardErasedSubType sameTypeAs CTList(CTBoolean) shouldBe true
  //    CTList(CTBoolean).nullable.wildcardErasedSubType sameTypeAs CTList(CTBoolean).nullable shouldBe true
  //  }
  //
  //  it("handling wildcard types") {
  //    (CTAny superTypeOf CTWildcard) shouldBe true
  //    (CTWildcard superTypeOf CTVoid) shouldBe true
  //    (CTWildcard superTypeOf CTAny) shouldBe Maybe
  //    (CTVoid superTypeOf CTWildcard) shouldBe Maybe
  //
  //    (CTAny subTypeOf CTWildcard) shouldBe Maybe
  //    (CTWildcard subTypeOf CTVoid) shouldBe Maybe
  //    (CTWildcard subTypeOf CTAny) shouldBe true
  //    (CTVoid subTypeOf CTWildcard) shouldBe true
  //
  //    materialTypes.foreach { t =>
  //      (t union CTWildcard).wildcardErasedSuperType shouldBe CTAny
  //    }
  //    materialTypes.foreach { t =>
  //      (t intersect CTWildcard).wildcardErasedSubType shouldBe CTVoid
  //    }
  //
  //    materialTypes.foreach { t =>
  //      (t union CTWildcard.nullable).wildcardErasedSuperType shouldBe CTAny.nullable
  //    }
  //    materialTypes.foreach { t =>
  //      (t intersect CTWildcard.nullable).wildcardErasedSubType shouldBe CTVoid
  //    }
  //
  //    nullableTypes.foreach { t =>
  //      (t union CTWildcard.nullable).wildcardErasedSuperType shouldBe CTAny.nullable
  //    }
  //    nullableTypes.foreach { t =>
  //      (t intersect CTWildcard.nullable).wildcardErasedSubType shouldBe CTNull
  //    }
  //
  //    nullableTypes.foreach { t =>
  //      (t union CTWildcard).wildcardErasedSuperType shouldBe CTAny.nullable
  //    }
  //    nullableTypes.foreach { t =>
  //      (t intersect CTWildcard).wildcardErasedSubType shouldBe CTVoid
  //    }
  //  }
  //
  //  it("contains wildcard") {
  //    CTNode.containsWildcard shouldBe false
  //    CTWildcard.containsWildcard shouldBe true
  //    CTWildcard.nullable.containsWildcard shouldBe true
  //    CTList(CTAny).containsWildcard shouldBe false
  //    CTList(CTList(CTWildcard)).containsWildcard shouldBe true
  //    CTList(CTList(CTWildcard.nullable)).containsWildcard shouldBe true
  //  }
  //
  //  it("contains nullable") {
  //    CTNode.containsNullable shouldBe false
  //    CTNode.nullable.containsNullable shouldBe true
  //    CTWildcard.containsNullable shouldBe false
  //    CTWildcard.nullable.containsNullable shouldBe true
  //    CTList(CTAny).containsNullable shouldBe false
  //    CTList(CTList(CTWildcard)).containsNullable shouldBe false
  //    CTList(CTList(CTWildcard.nullable)).containsNullable shouldBe true
  //  }
  //
  //  it("is inhabited") {
  //    allTypes.foreach {
  //      case t @ CTAny      => t.isInhabited should be(true)
  //      case t @ CTVoid     => t.isInhabited should be(false)
  //      case t @ CTWildcard => t.isInhabited should be(Maybe)
  //      case t              => t.isInhabited should be(true)
  //    }
  //  }
  //
  //  it("as nullable as") {
  //    materialTypes.foreach { t =>
  //      materialTypes.foreach { m =>
  //        m.asNullableAs(t) should equal(m)
  //      }
  //      nullableTypes.foreach { n =>
  //        n.asNullableAs(t) should equal(n.material)
  //      }
  //    }
  //
  //    nullableTypes.foreach { t =>
  //      materialTypes.foreach { m =>
  //        m.asNullableAs(t) should equal(m.nullable)
  //      }
  //      nullableTypes.foreach { n =>
  //        n.asNullableAs(t) should equal(n)
  //      }
  //    }
  //  }
}
