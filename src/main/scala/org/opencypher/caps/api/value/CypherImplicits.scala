package org.opencypher.caps.api.value

trait CypherImplicits
  extends CypherValue.Encoders
    with CypherValue.Conversion
    with EntityData.Creation
