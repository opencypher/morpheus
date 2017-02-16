package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast

class PatternConverter {

  def convert(p: ast.Pattern): Set[AnyEntity] = p.patternParts.foldLeft(State.empty) {
    case (state, part) => convert(state, part)
  }.unpack

  private def convert(s: State, p: ast.PatternPart): State = p match {
    case _: ast.AnonymousPatternPart => convert(s, p.element)
    case ast.NamedPatternPart(_, part) => convert(s, part)
  }

  private def convert(s: State, p: ast.PatternElement): State = p match {
    case ast.NodePattern(Some(v), _, None) => s.withEntity(v.name)
    case ast.RelationshipChain(left, rel, right) =>
      val state1 = convert(s, left)
      val state2 = convert(state1, rel)
      convert(state2, right)
    case _ => s
  }

  private def convert(s: State, r: ast.RelationshipPattern): State = {
    // extract predicates from here?
    s.withRel(r.variable.get.name)
  }

  object State {
    val empty = State(Set.empty)
  }

  final case class State(ents: Set[AnyEntity]) {
    def withEntity(node: String) = copy(ents = ents + AnyNode(Field(node)))

    def withRel(rel: String) = copy(ents = ents + AnyRelationship(Field(rel)))

    def unpack: Set[AnyEntity] = ents
  }

}
