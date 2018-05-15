package org.opencypher.spark.api.io.util

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import ColumnUtils._

class ColumnUtilsTest extends FunSpec with GeneratorDrivenPropertyChecks with Matchers {

  it("encodes arbitrary strings with only letters, digits, underscores, and 'at' symbols") {
    forAll { s: String =>
      val encoded = s.encodeToSQLCompatible
      val decoded = encoded.decodeFromSQLCompatible
      s should equal(decoded)
      encoded.forall { c =>
        c.isLetterOrDigit || c == '_' || c == '@'
      }
    }
  }

}
