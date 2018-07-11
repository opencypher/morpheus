package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.table.FlatRelationalTable

trait RelationalCypherResult[T <: FlatRelationalTable[T]] extends CypherResult {

  override type Graph <: RelationalCypherGraph[T]

}
