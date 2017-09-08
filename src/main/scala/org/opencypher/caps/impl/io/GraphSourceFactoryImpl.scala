package org.opencypher.caps.impl.io

import java.net.URI

import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory, GraphSourceFactoryCompanion}
import org.opencypher.caps.impl.exception.Raise

abstract class GraphSourceFactoryImpl[Source <: GraphSource](val companion: GraphSourceFactoryCompanion)
  extends GraphSourceFactory {

  override def schemes: Set[String] = companion.supportedSchemes

  override final def sourceFor(uri: URI): Source =
    if (schemes.contains(uri.getScheme)) sourceForVerifiedURI(uri)
    else Raise.graphSourceSchemeNotSupported(uri, schemes)

  protected def sourceForVerifiedURI(uri: URI): Source
}
