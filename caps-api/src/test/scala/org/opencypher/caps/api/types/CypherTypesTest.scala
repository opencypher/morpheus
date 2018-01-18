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
package org.opencypher.caps.api.types

import org.opencypher.caps.test.BaseTestSuite

import scala.language.postfixOps

class CypherTypesTest extends BaseTestSuite {

  val materialTypes: Seq[MaterialCypherType] = Seq(
    CTAny,
    CTBoolean,
    CTNumber,
    CTInteger,
    CTFloat,
    CTString,
    CTMap,
    CTNode,
    CTNode("Person"),
    CTNode("Person", "Employee"),
    CTRelationship,
    CTRelationship("KNOWS"),
    CTRelationship("KNOWS", "LOVES"),
    CTPath,
    CTList(CTAny),
    CTList(CTList(CTBoolean)),
    CTList(CTWildcard),
    CTWildcard,
    CTVoid
  )

  val nullableTypes: Seq[NullableCypherType] =
    materialTypes.map(_.nullable)

  val allTypes: Seq[CypherType] =
    materialTypes ++ nullableTypes

  test("couldBe") {
    CTAny couldBeSameTypeAs CTNode shouldBe true
    CTNode couldBeSameTypeAs CTAny shouldBe true
    CTInteger couldBeSameTypeAs CTNumber shouldBe true
    CTNumber couldBeSameTypeAs CTInteger shouldBe true
    CTFloat couldBeSameTypeAs CTInteger shouldBe false
    CTBoolean couldBeSameTypeAs CTInteger shouldBe false

    CTNode couldBeSameTypeAs CTMap shouldBe true
    CTRelationship couldBeSameTypeAs CTNode shouldBe false
    CTRelationship couldBeSameTypeAs CTMap shouldBe true

    CTList(CTInteger) couldBeSameTypeAs CTList(CTFloat) shouldBe false
    CTList(CTInteger) couldBeSameTypeAs CTList(CTAny) shouldBe true
    CTList(CTAny) couldBeSameTypeAs CTList(CTInteger) shouldBe true
  }

  test("joining with list of void") {
    val voidList = CTList(CTVoid)
    val otherList = CTList(CTString).nullable

    voidList join otherList should equal(otherList)
    otherList join voidList should equal(otherList)
  }

  test("type names") {
    Seq[(CypherType, (String, String))](
      CTAny -> ("ANY" -> "ANY?"),
      CTString -> ("STRING" -> "STRING?"),
      CTBoolean -> ("BOOLEAN" -> "BOOLEAN?"),
      CTNumber -> ("NUMBER" -> "NUMBER?"),
      CTInteger -> ("INTEGER" -> "INTEGER?"),
      CTFloat -> ("FLOAT" -> "FLOAT?"),
      CTMap -> ("MAP" -> "MAP?"),
      CTNode -> ("NODE" -> "NODE?"),
      CTNode("Person") -> (":Person NODE" -> ":Person NODE?"),
      CTNode("Person", "Employee") -> (":Person:Employee NODE" -> ":Person:Employee NODE?"),
      CTRelationship -> ("RELATIONSHIP" -> "RELATIONSHIP?"),
      CTRelationship("KNOWS") -> (":KNOWS RELATIONSHIP" -> ":KNOWS RELATIONSHIP?"),
      CTRelationship("KNOWS", "LOVES") -> (":KNOWS|LOVES RELATIONSHIP" -> ":KNOWS|LOVES RELATIONSHIP?"),
      CTPath -> ("PATH" -> "PATH?"),
      CTList(CTInteger) -> ("LIST OF INTEGER" -> "LIST? OF INTEGER"),
      CTList(CTInteger.nullable) -> ("LIST OF INTEGER?" -> "LIST? OF INTEGER?"),
      CTWildcard -> ("?" -> "??")
    ).foreach {
      case (t, (materialName, nullableName)) =>
        t.isMaterial shouldBe true
        t.toString shouldBe materialName
        t.nullable.toString shouldBe nullableName
    }

    CTVoid.toString shouldBe "VOID"
    CTNull.toString shouldBe "NULL"
  }

  test("RELATIONSHIP type") {
    CTRelationship().superTypeOf(CTRelationship()) shouldBe True
    CTRelationship().superTypeOf(CTRelationship("KNOWS")) shouldBe True
    CTRelationship("KNOWS").superTypeOf(CTRelationship()) shouldBe False
    CTRelationship().subTypeOf(CTRelationship("KNOWS")) shouldBe False
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS")) shouldBe True
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS", "LOVES")) shouldBe False
    CTRelationship("KNOWS", "LOVES").superTypeOf(CTRelationship("LOVES")) shouldBe True
    CTRelationship("KNOWS").superTypeOf(CTRelationship("NOSE")) shouldBe False
  }

  test("RELATIONSHIP? type") {
    CTRelationshipOrNull().superTypeOf(CTRelationshipOrNull()) shouldBe True
    CTRelationshipOrNull().superTypeOf(CTRelationshipOrNull("KNOWS")) shouldBe True
    CTRelationshipOrNull("KNOWS").superTypeOf(CTRelationshipOrNull("KNOWS")) shouldBe True
    CTRelationshipOrNull("KNOWS").superTypeOf(CTRelationshipOrNull("KNOWS", "LOVES")) shouldBe False
    CTRelationshipOrNull("KNOWS", "LOVES").superTypeOf(CTRelationshipOrNull("LOVES")) shouldBe True
    CTRelationshipOrNull("KNOWS").superTypeOf(CTRelationshipOrNull("NOSE")) shouldBe False
    CTRelationshipOrNull("FOO").superTypeOf(CTNull) shouldBe True
  }

  test("NODE type") {
    CTNode().superTypeOf(CTNode()) shouldBe True
    CTNode().superTypeOf(CTNode("Person")) shouldBe True
    CTNode("Person").superTypeOf(CTNode()) shouldBe False
    CTNode().subTypeOf(CTNode("Person")) shouldBe False
    CTNode("Person").superTypeOf(CTNode("Person")) shouldBe True
    CTNode("Person").superTypeOf(CTNode("Person", "Employee")) shouldBe True
    CTNode("Person", "Employee").superTypeOf(CTNode("Employee")) shouldBe False
    CTNode("Person").superTypeOf(CTNode("Foo")) shouldBe False
    CTNode("Person").superTypeOf(CTNode) shouldBe False
  }

  test("NODE? type") {
    CTNodeOrNull().superTypeOf(CTNodeOrNull()) shouldBe True
    CTNodeOrNull().superTypeOf(CTNodeOrNull("Person")) shouldBe True
    CTNodeOrNull("Person").superTypeOf(CTNodeOrNull("Person")) shouldBe True
    CTNodeOrNull("Person").superTypeOf(CTNodeOrNull("Person", "Employee")) shouldBe True
    CTNodeOrNull("Person", "Employee").superTypeOf(CTNodeOrNull("Employee")) shouldBe False
    CTNodeOrNull("Person").superTypeOf(CTNodeOrNull("Foo")) shouldBe False
    CTNodeOrNull("Foo").superTypeOf(CTNull) shouldBe True
  }

  test("conversion between VOID and NULL") {
    CTVoid.nullable shouldBe CTNull
    CTNull.material shouldBe CTVoid
  }

  test("all nullable types contain their material types") {
    materialTypes.foreach(t => t.nullable superTypeOf t)
    materialTypes.foreach(t => t subTypeOf t.nullable)
  }

  test("conversion between material and nullable types") {
    materialTypes.foreach(t => t.nullable.material == t)
    nullableTypes.foreach(t => t.material.nullable == t)
  }

  test("subTypeOf as the inverse of superTypeOf") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        t1 subTypeOf t2 should be(t2 superTypeOf t1)
        t2 subTypeOf t1 should be(t1 superTypeOf t2)
      }
    }
  }

  test("basic type inheritance") {
    CTNumber superTypeOf CTInteger shouldBe True
    CTNumber superTypeOf CTFloat shouldBe True
    CTMap superTypeOf CTMap shouldBe True
    CTMap superTypeOf CTNode shouldBe True
    CTMap superTypeOf CTRelationship shouldBe True

    CTAny superTypeOf CTInteger shouldBe True
    CTAny superTypeOf CTFloat shouldBe True
    CTAny superTypeOf CTNumber shouldBe True
    CTAny superTypeOf CTBoolean shouldBe True
    CTAny superTypeOf CTMap shouldBe True
    CTAny superTypeOf CTNode shouldBe True
    CTAny superTypeOf CTRelationship shouldBe True
    CTAny superTypeOf CTPath shouldBe True
    CTAny superTypeOf CTList(CTAny) shouldBe True
    CTAny superTypeOf CTVoid shouldBe True

    CTList(CTNumber) superTypeOf CTList(CTInteger) shouldBe True

    CTVoid subTypeOf CTInteger shouldBe True
    CTVoid subTypeOf CTFloat shouldBe True
    CTVoid subTypeOf CTNumber shouldBe True
    CTVoid subTypeOf CTBoolean shouldBe True
    CTVoid subTypeOf CTMap shouldBe True
    CTVoid subTypeOf CTNode shouldBe True
    CTVoid subTypeOf CTRelationship shouldBe True
    CTVoid subTypeOf CTPath shouldBe True
    CTVoid subTypeOf CTList(CTAny) shouldBe True
    CTVoid subTypeOf CTVoid shouldBe True
    CTVoid subTypeOf CTList(CTInteger) shouldBe True

    CTBoolean.nullable superTypeOf CTAny shouldBe False
    CTAny superTypeOf CTBoolean.nullable shouldBe False
  }

  test("join") {
    CTInteger join CTFloat shouldBe CTNumber
    CTFloat join CTInteger shouldBe CTNumber
    CTNumber join CTFloat shouldBe CTNumber
    CTNumber join CTInteger shouldBe CTNumber
    CTNumber join CTString shouldBe CTAny

    CTNode join CTRelationship shouldBe CTMap
    CTNode join CTMap shouldBe CTMap
    CTString join CTBoolean shouldBe CTAny
    CTAny join CTInteger shouldBe CTAny

    CTList(CTInteger) join CTList(CTFloat) shouldBe CTList(CTNumber)
    CTList(CTInteger) join CTNode shouldBe CTAny

    CTAny join CTWildcard shouldBe CTAny
    CTAny join CTVoid shouldBe CTAny
    CTWildcard join CTAny shouldBe CTAny
    CTVoid join CTAny shouldBe CTAny

    CTNode("Car") join CTNode shouldBe CTNode
    CTNode join CTNode("Person") shouldBe CTNode
  }

  test("join with nullables") {
    CTInteger join CTFloat.nullable shouldBe CTNumber.nullable
    CTFloat.nullable join CTInteger.nullable shouldBe CTNumber.nullable
    CTNumber.nullable join CTString shouldBe CTAny.nullable

    CTNode join CTRelationship.nullable shouldBe CTMap.nullable
    CTNode.nullable join CTMap shouldBe CTMap.nullable
    CTString.nullable join CTBoolean.nullable shouldBe CTAny.nullable
    CTAny join CTInteger.nullable shouldBe CTAny.nullable
  }

  test("join with labels and types") {
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

    CTNode("Person") join CTRelationship("KNOWS") shouldBe CTMap
    CTNode("Person") join CTRelationship shouldBe CTMap
    CTRelationship("KNOWS") join CTNode("Person") shouldBe CTMap
    CTRelationship("KNOWS") join CTNode shouldBe CTMap
  }

  test("meet") {
    CTInteger meet CTNumber shouldBe CTInteger
    CTAny meet CTNumber shouldBe CTNumber

    CTWildcard meet CTNumber shouldBe CTWildcard

    CTList(CTInteger) meet CTList(CTFloat) shouldBe CTList(CTVoid)
    CTList(CTInteger) meet CTNode shouldBe CTVoid
    CTList(CTWildcard) meet CTList(CTNumber) shouldBe CTList(CTWildcard)

    CTVoid meet CTInteger shouldBe CTVoid
    CTVoid meet CTWildcard shouldBe CTVoid
    CTVoid meet CTAny shouldBe CTVoid

    CTInteger meet CTVoid shouldBe CTVoid
    CTWildcard meet CTVoid shouldBe CTVoid

    CTNode meet CTNode("Person") shouldBe CTNode("Person")
  }

  test("meet with labels and types") {
    CTMap meet CTNode shouldBe CTNode
    CTMap meet CTNode("Person") shouldBe CTNode("Person")
    CTMap meet CTRelationship("KNOWS") shouldBe CTRelationship("KNOWS")

    CTNode("Person") meet CTNode shouldBe CTNode("Person")
    CTNode("Person") meet CTNode("Foo") shouldBe CTNode("Person", "Foo")
    CTNode("Person", "Foo") meet CTNode("Foo") shouldBe CTNode("Person", "Foo")

    CTRelationship("KNOWS") meet CTRelationship shouldBe CTRelationship("KNOWS")
    CTRelationship("KNOWS") meet CTRelationship("LOVES") shouldBe CTVoid
    CTRelationship("KNOWS", "LOVES") meet CTRelationship("LOVES") shouldBe CTRelationship("LOVES")
  }

  test("type equality between different types") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        val result = t1 sameTypeAs t2
        (result isDefinite) should be(
          (t1.isDefinite && t2.isDefinite) ||
            (t1.isNullable && t2.isMaterial) ||
            (t1.isMaterial && t2.isNullable)
        )

        result match {
          case True =>
            (t1 subTypeOf t2).maybeTrue shouldBe true
            (t2 subTypeOf t1).maybeTrue shouldBe true
            (t1 superTypeOf t2).maybeTrue shouldBe true
            (t2 superTypeOf t1).maybeTrue shouldBe true

          case False =>
            if (t1 subTypeOf t2 isTrue)
              (t2 subTypeOf t1).maybeFalse shouldBe true

            if (t2 subTypeOf t1 isTrue)
              (t1 subTypeOf t2).maybeFalse shouldBe true

            if (t1 superTypeOf t2 isTrue)
              (t2 superTypeOf t1).maybeFalse shouldBe true

            if (t2 superTypeOf t1 isTrue)
              (t1 superTypeOf t2).maybeFalse shouldBe true

          case Maybe =>
            (
              (t1.isWildcard || t2.isWildcard) ||
                (t1.isNullable && t2.isMaterial) ||
                (t1.isMaterial && t2.isNullable)
            ) shouldBe true
        }
      }
    }
  }

  test("antisymmetry of subtyping") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 subTypeOf t2 isTrue) (t2 subTypeOf t1 isTrue) shouldBe (t2 sameTypeAs t1 isTrue)
        if (t1 superTypeOf t2 isTrue) (t2 superTypeOf t1 isTrue) shouldBe (t2 sameTypeAs t1 isTrue)
      }
    }
  }

  test("type equality between the same type") {
    allTypes.foreach(t => t == t)
    allTypes.foreach(t => t superTypeOf t)
    allTypes.foreach(t => t subTypeOf t)
    allTypes.foreach(t => (t join t) == t)
    allTypes.foreach(t => (t meet t) == t)
  }

  test("computing definite types (type erasure)") {
    CTWildcard.wildcardErasedSuperType sameTypeAs CTAny shouldBe True
    CTWildcard.nullable.wildcardErasedSuperType sameTypeAs CTAny.nullable shouldBe True
    CTList(CTWildcard).wildcardErasedSuperType sameTypeAs CTList(CTAny) shouldBe True
    CTList(CTWildcard.nullable).wildcardErasedSuperType sameTypeAs CTList(CTAny.nullable) shouldBe True
    CTList(CTBoolean).wildcardErasedSuperType sameTypeAs CTList(CTBoolean) shouldBe True
    CTList(CTBoolean).nullable.wildcardErasedSuperType sameTypeAs CTList(CTBoolean).nullable shouldBe True

    CTWildcard.wildcardErasedSubType sameTypeAs CTVoid shouldBe True
    CTWildcard.nullable.wildcardErasedSubType sameTypeAs CTNull shouldBe True
    CTList(CTWildcard).wildcardErasedSubType sameTypeAs CTList(CTVoid) shouldBe True
    CTList(CTWildcard.nullable).wildcardErasedSubType sameTypeAs CTList(CTNull) shouldBe True
    CTList(CTBoolean).wildcardErasedSubType sameTypeAs CTList(CTBoolean) shouldBe True
    CTList(CTBoolean).nullable.wildcardErasedSubType sameTypeAs CTList(CTBoolean).nullable shouldBe True
  }

  test("handling wildcard types") {
    (CTAny superTypeOf CTWildcard) shouldBe True
    (CTWildcard superTypeOf CTVoid) shouldBe True
    (CTWildcard superTypeOf CTAny) shouldBe Maybe
    (CTVoid superTypeOf CTWildcard) shouldBe Maybe

    (CTAny subTypeOf CTWildcard) shouldBe Maybe
    (CTWildcard subTypeOf CTVoid) shouldBe Maybe
    (CTWildcard subTypeOf CTAny) shouldBe True
    (CTVoid subTypeOf CTWildcard) shouldBe True

    materialTypes.foreach { t =>
      (t join CTWildcard).wildcardErasedSuperType shouldBe CTAny
    }
    materialTypes.foreach { t =>
      (t meet CTWildcard).wildcardErasedSubType shouldBe CTVoid
    }

    materialTypes.foreach { t =>
      (t join CTWildcard.nullable).wildcardErasedSuperType shouldBe CTAny.nullable
    }
    materialTypes.foreach { t =>
      (t meet CTWildcard.nullable).wildcardErasedSubType shouldBe CTVoid
    }

    nullableTypes.foreach { t =>
      (t join CTWildcard.nullable).wildcardErasedSuperType shouldBe CTAny.nullable
    }
    nullableTypes.foreach { t =>
      (t meet CTWildcard.nullable).wildcardErasedSubType shouldBe CTNull
    }

    nullableTypes.foreach { t =>
      (t join CTWildcard).wildcardErasedSuperType shouldBe CTAny.nullable
    }
    nullableTypes.foreach { t =>
      (t meet CTWildcard).wildcardErasedSubType shouldBe CTVoid
    }
  }

  test("contains wildcard") {
    CTNode.containsWildcard shouldBe false
    CTWildcard.containsWildcard shouldBe true
    CTWildcard.nullable.containsWildcard shouldBe true
    CTList(CTAny).containsWildcard shouldBe false
    CTList(CTList(CTWildcard)).containsWildcard shouldBe true
    CTList(CTList(CTWildcard.nullable)).containsWildcard shouldBe true
  }

  test("contains nullable") {
    CTNode.containsNullable shouldBe false
    CTNode.nullable.containsNullable shouldBe true
    CTWildcard.containsNullable shouldBe false
    CTWildcard.nullable.containsNullable shouldBe true
    CTList(CTAny).containsNullable shouldBe false
    CTList(CTList(CTWildcard)).containsNullable shouldBe false
    CTList(CTList(CTWildcard.nullable)).containsNullable shouldBe true
  }

  test("is inhabited") {
    allTypes.foreach {
      case t @ CTAny      => t.isInhabited should be(True)
      case t @ CTVoid     => t.isInhabited should be(False)
      case t @ CTWildcard => t.isInhabited should be(Maybe)
      case t              => t.isInhabited should be(True)
    }
  }

  test("as nullable as") {
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
}
