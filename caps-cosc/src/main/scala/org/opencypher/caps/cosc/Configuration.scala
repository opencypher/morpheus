package org.opencypher.caps.cosc

import org.opencypher.caps.demo.Configuration.ConfigOption

import scala.util.Try

object Configuration {
  object PrintCOSCPlan extends ConfigOption("caps.explainCosc", false)(s => Try(s.toBoolean).toOption) {
    def set(): Unit = set(true.toString)
  }
}
