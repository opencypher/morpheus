package org.opencypher.spark

import org.opencypher.spark.CypherTypes._
import org.scalatest.{FunSuite, Matchers}

class CypherTypesTest extends FunSuite with Matchers {

  val materialTypes: Seq[MaterialCypherType] = Seq(
    CTAny, CTBoolean, CTNumber, CTInteger, CTFloat, CTString, CTMap,
    CTNode, CTRelationship, CTPath,
    CTList(CTAny),
    CTWildcard,
    CTVoid
  )

  val nullableTypes: Seq[NullableCypherType] =
    materialTypes.map(_.nullable)

  val allTypes: Seq[CypherType] =
    materialTypes ++ nullableTypes

  test("type name test") {
    Seq[(CypherType, (String, String))](
      CTAny -> ("ANY", "ANY?"),
      CTString -> ("STRING", "STRING?"),
      CTBoolean -> ("BOOLEAN", "BOOLEAN?"),
      CTNumber -> ("NUMBER", "NUMBER?"),
      CTInteger -> ("INTEGER", "INTEGER?"),
      CTFloat -> ("FLOAT", "FLOAT?"),
      CTMap -> ("MAP" , "MAP?"),
      CTNode -> ("NODE", "NODE?"),
      CTRelationship -> ("RELATIONSHIP", "RELATIONSHIP?"),
      CTPath -> ("PATH", "PATH?"),
      CTList(CTInteger) -> ("LIST OF INTEGER", "LIST? OF INTEGER"),
      CTList(CTInteger.nullable) -> ("LIST OF INTEGER?", "LIST? OF INTEGER?"),
      CTWildcard -> ("?", "??")
    ).foreach {
      case (t, (materialName, nullableName)) =>

        t.isMaterial shouldBe true
        t.toString shouldBe materialName
        t.nullable.toString shouldBe nullableName
    }

    CTVoid.toString shouldBe "VOID"
    CTNull.toString shouldBe "NULL"
  }

  test("void and null conversion") {
    CTVoid.nullable shouldBe CTNull
    CTNull.material shouldBe CTVoid
  }

  test("all nullable types contain their material types") {
    materialTypes.foreach(t => t.nullable superTypeOf t)
    materialTypes.foreach(t => t subTypeOf t.nullable)
  }

  test("can convert between material and nullable types") {
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

  test("basic inheritance cases") {
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

  test("same type as") {
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
            t1 subTypeOf t2 shouldBe True
            t2 subTypeOf t1 shouldBe True
            t1 superTypeOf t2 shouldBe True
            t2 superTypeOf t1 shouldBe True

          case False =>
            if (t1 subTypeOf t2 isTrue)
              (t2 subTypeOf t1) shouldBe False

            if (t2 subTypeOf t1 isTrue)
              (t1 subTypeOf t2) shouldBe False

            if (t1 superTypeOf t2 isTrue)
              (t2 superTypeOf t1) shouldBe False

            if (t2 superTypeOf t1 isTrue)
              (t1 superTypeOf t2) shouldBe False

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

  test("positive antisymmetry") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 subTypeOf t2 isTrue) (t2 subTypeOf t1 isTrue) shouldBe (t2 sameTypeAs t1 isTrue)
        if (t1 superTypeOf t2 isTrue) (t2 superTypeOf t1 isTrue) shouldBe (t2 sameTypeAs t1 isTrue)
      }
    }
  }

  test("type equality") {
    allTypes.foreach(t => t == t)
    allTypes.foreach(t => t superTypeOf  t)
    allTypes.foreach(t => t subTypeOf t)
    allTypes.foreach(t => (t join t) == t)
    allTypes.foreach(t => (t meet t) == t)
  }

  test("erasure") {
    CTWildcard.definiteSuperType sameTypeAs CTAny shouldBe True
    CTWildcard.nullable.definiteSuperType sameTypeAs CTAny.nullable shouldBe True
    CTList(CTWildcard).definiteSuperType sameTypeAs CTList(CTAny) shouldBe True
    CTList(CTWildcard.nullable).definiteSuperType sameTypeAs CTList(CTAny.nullable) shouldBe True
    CTList(CTBoolean).definiteSuperType sameTypeAs CTList(CTBoolean) shouldBe True
    CTList(CTBoolean).nullable.definiteSuperType sameTypeAs CTList(CTBoolean).nullable shouldBe True

    CTWildcard.definiteSubType sameTypeAs CTVoid shouldBe True
    CTWildcard.nullable.definiteSubType sameTypeAs CTNull shouldBe True
    CTList(CTWildcard).definiteSubType sameTypeAs CTList(CTVoid) shouldBe True
    CTList(CTWildcard.nullable).definiteSubType sameTypeAs CTList(CTNull) shouldBe True
    CTList(CTBoolean).definiteSubType sameTypeAs CTList(CTBoolean) shouldBe True
    CTList(CTBoolean).nullable.definiteSubType sameTypeAs CTList(CTBoolean).nullable shouldBe True
  }

  test("unknown") {
    (CTAny superTypeOf CTWildcard) shouldBe True
    (CTWildcard superTypeOf CTVoid) shouldBe True
    (CTWildcard superTypeOf CTAny) shouldBe Maybe
    (CTVoid superTypeOf CTWildcard) shouldBe Maybe

    (CTAny subTypeOf CTWildcard) shouldBe Maybe
    (CTWildcard subTypeOf CTVoid) shouldBe Maybe
    (CTWildcard subTypeOf CTAny) shouldBe True
    (CTVoid subTypeOf CTWildcard) shouldBe True

    materialTypes.foreach { t => (t join CTWildcard).definiteSuperType shouldBe CTAny }
    materialTypes.foreach { t => (t meet CTWildcard).definiteSubType shouldBe CTVoid }

    materialTypes.foreach { t => (t join CTWildcard.nullable).definiteSuperType shouldBe CTAny.nullable }
    materialTypes.foreach { t => (t meet CTWildcard.nullable).definiteSubType shouldBe CTVoid }

    nullableTypes.foreach { t => (t join CTWildcard.nullable).definiteSuperType shouldBe CTAny.nullable }
    nullableTypes.foreach { t => (t meet CTWildcard.nullable).definiteSubType shouldBe CTNull }

    nullableTypes.foreach { t => (t join CTWildcard).definiteSuperType shouldBe CTAny.nullable }
    nullableTypes.foreach { t => (t meet CTWildcard).definiteSubType shouldBe CTVoid }
  }
}
