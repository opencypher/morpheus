package org.opencypher.caps.cosc.impl

import java.net.URI

import org.opencypher.caps.api.physical.RuntimeContext
import org.opencypher.caps.api.value.CypherValue.CypherMap

object COSCRuntimeContext {
  val empty = COSCRuntimeContext(CypherMap.empty, _ => None)
}

case class COSCRuntimeContext(
  parameters: CypherMap,
  resolve: URI => Option[COSCGraph]
) extends RuntimeContext[COSCRecords, COSCGraph]


