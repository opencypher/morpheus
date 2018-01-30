package org.opencypher.caps.logical.impl

sealed trait Direction

case object Outgoing extends Direction

case object Undirected extends Direction
