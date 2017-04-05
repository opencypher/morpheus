package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.{symbols => frontend}
import org.opencypher.spark.api.types._

// TODO: Should go to option I think
case object fromFrontendType extends (frontend.CypherType => CypherType) {
  override def apply(in: frontend.CypherType): CypherType = in match {
    case frontend.CTAny => CTAny
    case frontend.CTNumber => CTNumber
    case frontend.CTInteger => CTInteger
    case frontend.CTFloat => CTFloat
    case frontend.CTBoolean => CTBoolean
    case frontend.CTString => CTString
    case frontend.CTMap => CTMap
    case frontend.CTNode => CTNode
    case frontend.CTRelationship => CTRelationship
    case frontend.ListType(inner) => CTList(fromFrontendType(inner))
    case x => throw new UnsupportedOperationException(s"No support for type $x")
  }
}
