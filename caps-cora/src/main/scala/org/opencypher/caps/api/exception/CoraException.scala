package org.opencypher.caps.api.exception

import org.opencypher.caps.ir.api.expr.Var

abstract class CoraException(msg: String) extends CypherException(msg)

final case class RecordHeaderException(msg: String) extends CoraException(msg)

final case class DuplicateInputColumnException(columnName: String, entity: Var)
    extends CoraException(
      s"The input column '$columnName' is used more than once to describe the embedded entity $entity")
