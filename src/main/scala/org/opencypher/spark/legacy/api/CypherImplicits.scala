package org.opencypher.spark.legacy.api

import org.opencypher.spark.prototype.api.value.{CypherValue, EntityData}

trait CypherImplicits
  extends CypherValue.Encoders
    with CypherValue.Conversion
    with EntityData.Creation
