package org.opencypher.spark.api

import org.opencypher.spark.impl.newvalue.{CypherValue, EntityData}

trait CypherImplicits
  extends CypherValue.Encoders
    with CypherValue.Conversion
    with Ternary.Conversion
    with EntityId.Conversion
    with EntityData.Creation
