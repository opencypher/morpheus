package org.opencypher.caps

import org.opencypher.caps.api.ir.global.TokenRegistry
import org.opencypher.caps.api.spark.CAPSSession

object CAPSTestSession {
  trait Fixture {
    self: SparkTestSession.Fixture =>

    implicit lazy val caps = CAPSSession.create(session)

    def initialTokens = {
      TokenRegistry.empty
    }
  }
}
