package org.opencypher.okapi.impl.types

import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

object CypherTypeUtils {
  implicit class RichCypherType(val ct: CypherType) extends AnyVal {
    def toCTNode: CTNode = ct match {
      case n: CTNode => n
      case other => throw UnsupportedOperationException(s"cannot convert $other into a CTNode")
    }

    def toCTRelationship: CTRelationship = ct match {
      case r: CTRelationship => r
      case other => throw UnsupportedOperationException(s"cannot convert $other into a CTRelationship")
    }
  }
}
