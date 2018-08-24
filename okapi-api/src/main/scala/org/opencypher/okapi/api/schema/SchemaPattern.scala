package org.opencypher.okapi.api.schema

object SchemaPattern{
  def apply(
    sourceLabel: String,
    relType: String,
    targetLabel: String
  ): SchemaPattern = new SchemaPattern(Set(sourceLabel), relType, Set(targetLabel))
}

case class SchemaPattern(sourceLabels: Set[String], relType: String, targetLabels: Set[String]) {

}

//trait Cardinality
//case object Any extends Cardinality
//case object AtLeastOne extends Cardinality
//case object ExactlyOne extends Cardinality
//case class Between(from: Int, to: Int) extends Cardinality
//
//case class RelationshipConstraint(
//  startNodeLabelCombo: Set[String],
//  endNodeLabelCombo: Set[String],
//  startNodeCardinality: Cardinality,
//  endNodeCardinality: Cardinality
//) {
//
//  def hasOverlap(other: RelationshipConstraint): Boolean = {
//    startNodeLabelCombo.subsetOf(other.startNodeLabelCombo) || other.startNodeLabelCombo.subsetOf(startNodeLabelCombo)
//    endNodeLabelCombo.subsetOf(other.endNodeLabelCombo) || other.endNodeLabelCombo.subsetOf(endNodeLabelCombo)
//  }
//
//
//  def isSubTypeOf(other: RelationshipConstraint): Boolean = {
//    val startContained = startNodeLabelCombo.forall(other.startNodeLabelCombo.contains)
//    val endContained = endNodeLabelCombo.forall(other.endNodeLabelCombo.contains)
//
//    (startContained, endContained match {
//      case (true, true) =>
//    })
//  }
//}
//
//object RelationshipConstraints {
//  type RelationshipConstraints = Map[String, Set[RelationshipConstraint]]
//
//  implicit class RichRelationshipConstraint(map: RelationshipConstraints) {
//
//    def register(relType: String, constraint: RelationshipConstraint): RelationshipConstraints = {
//      val existing = map.getOrElse(relType, Set.empty)
//
//      // TODO what to do when there are cardinality conflicts?
//      map.updated(relType, existing + constraint)
//    }
//
//    def ++(other: RelationshipConstraints): RelationshipConstraints = {
//      val conflicts = map.keySet intersect other.keySet
//
//      conflicts.map { relType =>
//        val lhs = map(relType)
//        val rhs = map(relType)
//
//        lhs.
//      }
//    }
//  }
//}


// (A, B), (A), (B), (C), (D), (D, E)
//

// Closed World
// (A, B) -[:REL]-> (C) ++ (A, C) -[:REL]-> (D) => no conflict (A, B) -[:REL]-> (C), (A, C) -[:REL]-> (D)
// (A, B) -[:REL]-> (C) ++ (A, B) -[:REL]-> (D) => no conflict (A, B) -[:REL]-> (C|D) or (A, B) -[:REL]-> (C), (A, B) -[:REL]-> (D)

// Open World
// (A) -> (C)
// ()-[:REL]->(:B)


// possibleLabels(Option(N), Option(E), Option(N)): Set[Constraint]
// MATCH (:B)<-[:REL]-() =>


