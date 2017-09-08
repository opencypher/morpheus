package org.opencypher.caps.api.io

sealed trait PersistMode
case object Overwrite extends PersistMode
case object CreateOrFail extends PersistMode
