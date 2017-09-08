package org.opencypher.caps.api.spark.exception

import org.opencypher.caps.api.exception.CypherException

final case class CAPSException(msg: String) extends CypherException(msg)
