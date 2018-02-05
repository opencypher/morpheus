package org.opencypher.caps.cosc

import java.net.URI

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.value.CypherValue

object COSCRuntimeContext {
  val empty = COSCRuntimeContext(Map.empty, _ => None)
}

case class COSCRuntimeContext(
  parameters: Map[String, CypherValue],
  resolve: URI => Option[PropertyGraph]
)


