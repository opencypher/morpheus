package org.opencypher.caps.cosc.impl

import java.net.URI

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.value.CypherValue.CypherMap

object COSCRuntimeContext {
  val empty = COSCRuntimeContext(CypherMap.empty, _ => None)
}

case class COSCRuntimeContext(
  parameters: CypherMap,
  resolve: URI => Option[PropertyGraph]
)


