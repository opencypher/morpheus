package org.opencypher.github.release

import java.io.File
import java.nio.file.Paths

import com.softwaremill.sttp._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

object CreateGitHubRelease extends App {

  val parser = new scopt.OptionParser[Config]("Github Release") {
    opt[String]('v', "releaseVersion")
      .required()
      .action((version, config) => config.copy(releaseVersion = version))
      .text("The version for which a GitHub release is created.")

    opt[String]('b', "branch")
      .action((branch, config) => config.copy(branch = branch))
      .text("The branch for which the tag is created. Default is `master`.")

    opt[String]('t', "githubToken")
      .required()
      .action((token, config) => config.copy(token = token))
      .text("GitHub API token.")

    opt[String]('a', "assetFolder")
      .required()
      .action((path, config) => config.copy(assetFolder = path))
      .text("Path of the asset folder.")
  }

  parser.parse(args, Config()) match {
    case Some(config) => GithubRelease(config)
    case _ => ()
  }
}

case class GithubRelease(config: Config) {
  implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()
  val GH_API_URL = "https://api.github.com/repos/opencypher/cypher-for-apache-spark"
  val GH_UPLOAD_URL = "https://uploads.github.com/repos/opencypher/cypher-for-apache-spark"

  private val releaseId = createRelease
  uploadJars(releaseId)

  def createRelease: String = {
    val requestBody = GithubReleaseCreationRequest(
      config.releaseVersion,
      config.branch,
      config.releaseName,
      config.releaseBody,
      config.draft,
      config.preRelease
    )

    val request = sttp
      .body(requestBody.asJson.toString())
      .post(buildRequestUri(GH_API_URL, "/releases"))

    println(s"Sending creation request: $request")
    val response = request.send()
    println(s"Response to creation request: $response")

    val responseJson = parseResponse(response)
    responseJson.asObject.get("id").get.toString
  }

  def uploadJars(releaseId: String): Unit = {
    val clusterJarAsset = s"spark-cypher-${config.releaseVersion}-cluster.jar"
    val standaloneJarAsset = s"spark-cypher-${config.releaseVersion}.jar"

    val clusterJarPath = Paths.get(config.assetFolder, clusterJarAsset)
    val standaloneJarPath = Paths.get(config.assetFolder, standaloneJarAsset)

    def uploadAsset(releaseId: String, assetName: String, file: File) = {
      val requestUri = buildRequestUri(GH_UPLOAD_URL, s"/releases/$releaseId/assets", "name" -> assetName)
      val request = sttp
        .multipartBody(multipart("file_part", file))
        .contentType("application/java-archive")
        .post(requestUri)

      println(s"Sending upload request: $request")
      val response = request.send()
      println(s"Response to upload request: $response")

      parseResponse(response)
    }

    uploadAsset(releaseId, clusterJarAsset, clusterJarPath.toFile)
    uploadAsset(releaseId, standaloneJarAsset, standaloneJarPath.toFile)
  }

  private def buildRequestUri(base: String, path: String, parameters: (String, String)*): Uri = {
    val allParameters = parameters :+ ("access_token" -> config.token)
    val host = s"$base$path"
    uri"$host?$allParameters"
  }

  private def parseResponse(response: Response[String]): Json = response.body match {
    case Right(body) => parse(body).right.get
    case Left(error) => sys.error(s"Request returned an error: $error")
  }
}

case class Config(
  releaseVersion: String = "",
  branch: String = "master",
  token: String = "",
  assetFolder: String = ""
) {
  val releaseName: String = s"CAPS Release Version $releaseVersion"

  val releaseBody: String =
    s"""
       |### `spark-cypher-$releaseVersion`
       |
       |This is the artifact deployed to [Maven Central](https://search.maven.org/#artifactdetails%7Corg.opencypher%7Cspark-cypher%7C$releaseVersion%7Cjar). To use this release in a Maven project, add the following dependency to your pom:
       |```
       |<dependency>
       |  <groupId>org.opencypher</groupId>
       |  <artifactId>spark-cypher</artifactId>
       |  <version>$releaseVersion</version>
       |</dependency>
       |```
       |
       |For SBT:
       |```
       |libraryDependencies += "org.opencypher" % "spark-cypher" % "$releaseVersion"
       |```
       |
       |### `spark-cypher-$releaseVersion-cluster`
       |This is a fat jar that does not include the Spark dependencies. It is intended to be used in environments where Spark is already present, for example, a Spark cluster or a notebook.
       """.stripMargin

  val draft = false

  val preRelease: Boolean = releaseVersion.contains("-")
}

case class GithubReleaseCreationRequest(
  tag_name: String,
  target_commitish: String,
  name: String,
  body: String,
  draft: Boolean,
  prerelease: Boolean
)