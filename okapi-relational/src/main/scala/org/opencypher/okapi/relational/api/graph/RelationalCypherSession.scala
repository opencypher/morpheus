package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.CypherSession
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecordsFactory}

trait RelationalCypherSession[T <: FlatRelationalTable[T]] extends CypherSession {

  type Graph <: RelationalCypherGraph[T]

  private[opencypher] def records: RelationalCypherRecordsFactory[T]

  private[opencypher] def graphs: RelationalCypherGraphFactory[T]

}
