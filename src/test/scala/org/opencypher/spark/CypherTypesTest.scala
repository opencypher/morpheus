package org.opencypher.spark

import org.opencypher.spark.CypherTypes._
import org.scalatest.{Matchers, FunSuite}

class CypherTypesTest extends FunSuite with Matchers {

  val materialTypes = Seq(
    CTAny, CTBoolean, CTNumber, CTInteger, CTFloat, CTString, CTMap,
    CTNode, CTRelationship, CTPath,
    CTList(CTAny),
    CTVoid
  )

  val nullableTypes = materialTypes.map(_.orNull)

  val allTypes = materialTypes ++ nullableTypes

  test("type name test") {
    Seq(
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
      CTList(CTInteger.orNull) -> ("LIST OF INTEGER?", "LIST? OF INTEGER?")
    ).foreach {
      case (t, (materialName, nullableName)) =>

        t.isMaterial shouldBe true
        t.toString shouldBe materialName
        t.orNull.toString shouldBe nullableName
    }

    CTVoid.toString shouldBe "VOID"
    CTNull.toString shouldBe "NULL"
  }

  test("void and null conversion") {
    CTVoid.orNull shouldBe CTNull
    CTNull.withoutNull shouldBe CTVoid
  }

  test("all nullable types contain their material types") {
    materialTypes.foreach(t => t.orNull superTypeOf t)
    materialTypes.foreach(t => t subTypeOf t.orNull)
  }

  test("can convert between material and nullable types") {
    materialTypes.foreach(t => t.orNull.withoutNull == t)
    nullableTypes.foreach(t => t.withoutNull.orNull == t)
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
    CTNumber superTypeOf CTInteger shouldBe true
    CTNumber superTypeOf CTFloat shouldBe true
    CTMap superTypeOf CTMap shouldBe true
    CTMap superTypeOf CTNode shouldBe true
    CTMap superTypeOf CTRelationship shouldBe true

    CTAny superTypeOf CTInteger shouldBe true
    CTAny superTypeOf CTFloat shouldBe true
    CTAny superTypeOf CTNumber shouldBe true
    CTAny superTypeOf CTBoolean shouldBe true
    CTAny superTypeOf CTMap shouldBe true
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
    CTVoid subTypeOf CTMap shouldBe true
    CTVoid subTypeOf CTNode shouldBe true
    CTVoid subTypeOf CTRelationship shouldBe true
    CTVoid subTypeOf CTPath shouldBe true
    CTVoid subTypeOf CTList(CTAny) shouldBe true
    CTVoid subTypeOf CTVoid shouldBe true
    CTVoid subTypeOf CTList(CTInteger) shouldBe true

    CTBoolean.orNull superTypeOf CTAny  shouldBe false
    CTAny superTypeOf CTBoolean.orNull shouldBe false
  }

  test("reflexivity") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 == t2) {
          t1 subTypeOf t2 shouldBe true
          t2 subTypeOf t1 shouldBe true
          t1 superTypeOf t2 shouldBe true
          t2 superTypeOf t1 shouldBe true
        }
      }
    }
  }

  test("positive antisymmetry") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 subTypeOf t2) (t2 subTypeOf t1) shouldBe t2 == t1
        if (t1 superTypeOf t2) (t2 superTypeOf t1) shouldBe t2 == t1
      }
    }
  }

  test("type equality") {
    allTypes.foreach(t => t == t)
    allTypes.foreach(t => t subTypeOf t)
    allTypes.foreach(t => t subTypeOf t)
    allTypes.foreach(t => (t join t) == t)
    allTypes.foreach(t => (t meet t) == t)
  }
}
