package org.opencypher.okapi.ir.api.expr


sealed trait PropagationType
case object NullOrAnyNullable extends PropagationType
case object AnyNullable extends PropagationType
case object AllNullable extends PropagationType
