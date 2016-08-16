package org.opencypher.spark

import org.opencypher.spark.api.types._
import org.opencypher.spark.api._

import scala.language.postfixOps

class CypherTypesTest extends StdTestSuite {

  val materialTypes: Seq[MaterialCypherType] = Seq(
    CTAny, CTBoolean, CTNumber, CTInteger, CTFloat, CTString, CTMap,
    CTNode, CTRelationship, CTPath,
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
      CTRelationship -> ("RELATIONSHIP" -> "RELATIONSHIP?"),
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

    CTBoolean.nullable superTypeOf CTAny  shouldBe False
    CTAny superTypeOf CTBoolean.nullable shouldBe False
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
    allTypes.foreach(t => t superTypeOf  t)
    allTypes.foreach(t => t subTypeOf t)
    allTypes.foreach(t => (t join t) == t)
    allTypes.foreach(t => (t meet t) == t)
  }

  test("computing definite types (type erasure)") {
    CTWildcard.erasedSuperType sameTypeAs CTAny shouldBe True
    CTWildcard.nullable.erasedSuperType sameTypeAs CTAny.nullable shouldBe True
    CTList(CTWildcard).erasedSuperType sameTypeAs CTList(CTAny) shouldBe True
    CTList(CTWildcard.nullable).erasedSuperType sameTypeAs CTList(CTAny.nullable) shouldBe True
    CTList(CTBoolean).erasedSuperType sameTypeAs CTList(CTBoolean) shouldBe True
    CTList(CTBoolean).nullable.erasedSuperType sameTypeAs CTList(CTBoolean).nullable shouldBe True

    CTWildcard.erasedSubType sameTypeAs CTVoid shouldBe True
    CTWildcard.nullable.erasedSubType sameTypeAs CTNull shouldBe True
    CTList(CTWildcard).erasedSubType sameTypeAs CTList(CTVoid) shouldBe True
    CTList(CTWildcard.nullable).erasedSubType sameTypeAs CTList(CTNull) shouldBe True
    CTList(CTBoolean).erasedSubType sameTypeAs CTList(CTBoolean) shouldBe True
    CTList(CTBoolean).nullable.erasedSubType sameTypeAs CTList(CTBoolean).nullable shouldBe True
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

    materialTypes.foreach { t => (t join CTWildcard).erasedSuperType shouldBe CTAny }
    materialTypes.foreach { t => (t meet CTWildcard).erasedSubType shouldBe CTVoid }

    materialTypes.foreach { t => (t join CTWildcard.nullable).erasedSuperType shouldBe CTAny.nullable }
    materialTypes.foreach { t => (t meet CTWildcard.nullable).erasedSubType shouldBe CTVoid }

    nullableTypes.foreach { t => (t join CTWildcard.nullable).erasedSuperType shouldBe CTAny.nullable }
    nullableTypes.foreach { t => (t meet CTWildcard.nullable).erasedSubType shouldBe CTNull }

    nullableTypes.foreach { t => (t join CTWildcard).erasedSuperType shouldBe CTAny.nullable }
    nullableTypes.foreach { t => (t meet CTWildcard).erasedSubType shouldBe CTVoid }
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
      case t @ CTVoid => t.isInhabited should be(False)
      case t @ CTWildcard => t.isInhabited should be(Maybe)
      case t => t.isInhabited should be(True)
    }
  }
}
