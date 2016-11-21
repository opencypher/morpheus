package org.opencypher.spark.api.value

import org.opencypher.spark.api.types.CTFloat

object CypherTestValues {

  // This object provides various seqs of cypher values sorted by orderability for use in tests.
  //
  // List items are sequences of items that are equivalent.
  //
  // These inner seqs are supposed to be duplicate free
  //
  // Note: We can't use sets here as that would mean we'd use the equality in the test data that we're about to test
  //
  type ValueGroups[V] = Seq[Values[V]]
  type Values[V] = Seq[V]

  implicit val PATH_valueGroups: ValueGroups[CypherPath] = Seq(
    Seq(
      CypherPath(CypherNode(1l, Array("Label"), Properties.empty)),
      CypherPath(CypherNode(1l, Array("NotSignificant"), Properties.empty)),
      CypherPath(CypherNode(1l, Array("NotSignificant"), Properties("alsoNotSig" -> CypherBoolean(true))))
    ),
    Seq(CypherPath(CypherNode(1l, Array("Label"), Properties.empty),
                   CypherRelationship(100l, 1l, 2l, "KNOWS", Properties.empty),
                   CypherNode(2l, Array.empty, Properties.empty)),
        CypherPath(CypherNode(1l, Array("Label"), Properties.empty),
                   CypherRelationship(100l, 1l, 2l, "FORGETS", Properties.empty),
                   CypherNode(2l, Array.empty, Properties.empty))),
    Seq(CypherPath(CypherNode(1l, Array("Label"), Properties.empty),
                   CypherRelationship(100l, 1l, 2l, "KNOWS", Properties("aRelProp" -> CypherFloat(667.5))),
                   CypherNode(2l, Array.empty, Properties.empty),
                   CypherRelationship(100l, 1l, 2l, "KNOWS", Properties.empty),
                   CypherNode(2l, Array("One", "Two", "Three"), Properties.empty))),
    Seq(cypherNull[CypherPath])
  )

  implicit val RELATIONSHIP_valueGroups: ValueGroups[CypherRelationship] = Seq(
    Seq(
      CypherRelationship(EntityId(1), EntityId(1), EntityId(1), "KNOWS", Properties("a" -> CypherInteger(1), "b" -> null)),
      CypherRelationship(EntityId(1), EntityId(2), EntityId(4), "FORGETS", Properties("a" -> CypherFloat(1.0), "b" -> null))
      ),
    Seq(CypherRelationship(EntityId(10), EntityId(1), EntityId(1), "KNOWS", Properties("a" -> CypherInteger(1)))),
    Seq(CypherRelationship(EntityId(20), EntityId(1), EntityId(1), "KNOWS", Properties("a" -> CypherInteger(1), "b" -> CypherInteger(1)))),
    Seq(CypherRelationship(EntityId(21), EntityId(0), EntityId(-1), "KNOWS", Properties("b" -> null))),
    Seq(CypherRelationship(EntityId(30), EntityId(1), EntityId(1), "_-&", Properties.empty)),
    Seq(CypherRelationship(EntityId(40), EntityId(1), EntityId(1), "", Properties("c" -> CypherInteger(10), "b" -> null))),
    Seq(cypherNull[CypherRelationship])
  )

  implicit val NODE_valueGroups: ValueGroups[CypherNode] = Seq(
    Seq(
      CypherNode(EntityId(1), Array("Person"), Properties("a" -> CypherInteger(1), "b" -> null)),
      CypherNode(EntityId(1), Array("Person"), Properties("a" -> CypherFloat(1.0d), "b" -> null))
    ),
    Seq(CypherNode(EntityId(10), Array(), Properties("a" -> CypherInteger(1)))),
    Seq(CypherNode(EntityId(20), Array("MathGuy"), Properties("a" -> CypherInteger(1), "b" -> CypherInteger(1)))),
    Seq(CypherNode(EntityId(21), Array("MathGuy", "FanOfNulls"), Properties("b" -> null))),
    Seq(CypherNode(EntityId(30), Array("NoOne"), Properties.empty)),
    Seq(CypherNode(EntityId(40), Array(), Properties("c" -> CypherInteger(10), "b" -> null))),
    Seq(cypherNull[CypherNode])
  )

  implicit val MAP_valueGroups: ValueGroups[CypherMap] = Seq(
    // TODO: Add more nested examples
    Seq(CypherMap()),
    Seq(CypherMap("a" -> CypherInteger(1))),
    Seq(CypherMap("a" -> CypherInteger(1), "b" -> CypherInteger(1))),
    Seq(
      CypherMap("a" -> CypherInteger(1), "b" -> null),
      CypherMap("a" -> CypherFloat(1.0d), "b" -> null)
    ),
    Seq(CypherMap("b" -> null)),
    Seq(CypherMap("c" -> CypherInteger(10), "b" -> null)),
    Seq(cypherNull[CypherMap])
  )

  implicit val LIST_valueGroups: ValueGroups[CypherList] = Seq(
    // TODO: Add more nested examples
    Seq(CypherList(Seq())),
    Seq(CypherList(Seq(CypherInteger(1)))),
    Seq(CypherList(Seq(CypherInteger(1), CypherInteger(0)))),
    Seq(CypherList(Seq(CypherInteger(1), CypherFloat(0), CypherInteger(2)))),
    Seq(CypherList(Seq(CypherInteger(1), CypherFloat(0.5)))),
    Seq(CypherList(Seq(CypherInteger(1), CypherFloat(1.5)))),
    Seq(CypherList(Seq(CypherInteger(1), cypherNull[CypherNumber], CypherInteger(2)))),
    Seq(cypherNull[CypherList])
  )

  implicit val STRING_valueGroups: ValueGroups[CypherString] = Seq(
    Seq(CypherString("")),
    Seq(CypherString("  ")),
    Seq(CypherString("1234567890")),
    Seq(CypherString("A")),
    Seq(CypherString("AB")),
    Seq(CypherString("ABC")),
    Seq(CypherString("Is it a query, if no one sees it running?")),
    Seq(CypherString("a"), CypherString("a")),
    Seq(cypherNull[CypherString])
  )

  implicit val BOOLEAN_valueGroups: ValueGroups[CypherBoolean] = Seq(
    Seq(CypherBoolean(false), CypherBoolean(false)),
    Seq(CypherBoolean(true)),
    Seq(cypherNull[CypherBoolean])
  )

  implicit val INTEGER_valueGroups: ValueGroups[CypherInteger] = Seq(
    Seq(CypherInteger(Long.MinValue)),
    Seq(CypherInteger(-23L)),
    Seq(CypherInteger(-10), CypherInteger(-10)),
    Seq(CypherInteger(-1)),
    Seq(CypherInteger(0)),
    Seq(CypherInteger(1)),
    Seq(CypherInteger(2)),
    Seq(CypherInteger(5), CypherInteger(5)),
    Seq(CypherInteger(42L)),
    Seq(CypherInteger(Long.MaxValue)),
    Seq(cypherNull[CypherInteger], cypherNull[CypherInteger])
  )

  implicit val FLOAT_valueGroups: ValueGroups[CypherFloat] = Seq(
    Seq(CypherFloat(Double.NegativeInfinity)),
    Seq(CypherFloat(Double.MinValue)),
    Seq(CypherFloat(-23.0d)),
    Seq(CypherFloat(-10.0d), CypherFloat(-10.0d)),
    Seq(CypherFloat(0.0d)),
    Seq(CypherFloat(2.3d)),
    Seq(CypherFloat(5.0d)),
    Seq(CypherFloat(5.1d), CypherFloat(5.1d)),
    Seq(CypherFloat(42.0d)),
    Seq(CypherFloat(Double.MaxValue)),
    Seq(CypherFloat(Double.PositiveInfinity)),
    Seq(CypherFloat(Double.NaN)),
    Seq(cypherNull[CypherFloat])
  )

  implicit val NUMBER_valueGroups: ValueGroups[CypherNumber] = Seq(
    Seq(CypherFloat(Double.NegativeInfinity)),
    Seq(CypherFloat(Double.MinValue)),
    Seq(CypherInteger(Long.MinValue)),
    Seq(CypherInteger(-23L), CypherFloat(-23.0d)),
    Seq(CypherFloat(-10.0d), CypherInteger(-10), CypherFloat(-10.0d), CypherInteger(-10)),
    Seq(CypherInteger(-1), CypherFloat(-1.0d)),
    Seq(CypherInteger(0), CypherFloat(0.0d)),
    Seq(CypherInteger(1)),
    Seq(CypherInteger(2)),
    Seq(CypherFloat(2.3d)),
    Seq(CypherInteger(5), CypherInteger(5), CypherFloat(5.0d)),
    Seq(CypherFloat(5.1d), CypherFloat(5.1d)),
    Seq(CypherFloat(42.0d), CypherInteger(42L)),
    Seq(CypherInteger(Long.MaxValue)),
    Seq(CypherFloat(Double.MaxValue)),
    Seq(CypherFloat(Double.PositiveInfinity)),
    Seq(CypherFloat(Double.NaN)),
    Seq(cypherNull[CypherFloat], cypherNull[CypherInteger], cypherNull[CypherNumber])
  )

  implicit val ANY_valueGroups: ValueGroups[CypherValue] = {
    val allGroups = Seq(
      MAP_valueGroups,
      NODE_valueGroups,
      RELATIONSHIP_valueGroups,
      PATH_valueGroups,
      LIST_valueGroups,
      STRING_valueGroups,
      BOOLEAN_valueGroups,
      NUMBER_valueGroups
    )

    val materials = allGroups.flatMap(_.materialValueGroups)
    val nulls = Seq(allGroups.flatMap(_.nullableValueGroups).flatten)

    materials ++ nulls
  }

  implicit final class CypherValueGroups[V <: CypherValue](elts: ValueGroups[V]) {
    def materialValueGroups: ValueGroups[V] = elts.map(_.filter(_ != cypherNull)).filter(_.nonEmpty)
    def nullableValueGroups: ValueGroups[V] = elts.map(_.filter(_ == cypherNull)).filter(_.nonEmpty)
    def scalaValueGroups(implicit companion: CypherValueCompanion[V]): Seq[Seq[Any]] = elts.map(_.scalaValues.map(_.orNull))
    def indexed = elts.zipWithIndex.flatMap { case ((group), index) => group.map { v => index -> v } }
  }

  implicit final class CypherValues[V <: CypherValue](elts: Values[V]) {
    def scalaValues(implicit companion: CypherValueCompanion[V]): Seq[Option[Any]] = elts.map(companion.contents)
  }
}
