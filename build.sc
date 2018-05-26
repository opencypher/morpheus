import $file.shared
import mill._

object OkapiTrees extends shared.OkapiModule {

  object test extends OkapiTests

}

object OkapiApi extends shared.OkapiModule {

  override def moduleDeps = Seq(OkapiTrees)

  override def ivyDeps = super.ivyDeps() ++ Agg(cats)

  object test extends OkapiTests {
    override def ivyDeps = super.ivyDeps() ++ Agg(mockito)
  }

}

object OkapiTesting extends shared.OkapiModule {

  override def moduleDeps = Seq(OkapiApi)

  override def ivyDeps = super.ivyDeps() ++ Agg(
    frontend,
    bouncyCastle,
    scalaTest,
    neo4jHarness
  )

  object test extends OkapiTests

}
