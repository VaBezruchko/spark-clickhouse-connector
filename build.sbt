import sbt.Keys.{credentials, libraryDependencies, pomExtra, publishM2Configuration, run, version}
import Versions._
import sbt.Credentials
import sbtassembly.AssemblyKeys.{assembly, assemblyJarName, assemblyOption}


val nexus_local = IO.read(new File(Path.userHome.absolutePath + "/.sbt/nexus_url"))
lazy val creds = Seq(credentials += Credentials(Path.userHome / ".sbt" / "credentials"))


val commonSettings = creds ++ Seq(
  name :=  "spark-clickhouse-connector",
  organization := "io.clickhouse",
  version := "0.22",
  scalaVersion := "2.11.12",
  publishMavenStyle := true,

  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishM2Configuration := publishM2Configuration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),

  publishTo := {
    if (isSnapshot.value)
      Some("Snapshot repository" at nexus_local+ "/content/repositories/snapshots/")
    else
      Some("Release repository" at nexus_local + "/content/repositories/releases/")
  },

    pomExtra :=
    <developers>
      <developer>
        <id>vbezruchko</id>
        <name>Vadim Bezruchko</name>
        <email>va.bezruchko@gmail.com</email>
      </developer>
    </developers>

)



val deps = Seq (
  "org.apache.spark"        %%  "spark-core"             % Spark           % "provided",
  "org.apache.spark"        %%  "spark-sql"              % Spark           % "provided",

  "org.slf4j"               %   "slf4j-api"              % Slf4j           % "provided",

  "org.eclipse.jetty"       %   "jetty-server"           % SparkJetty      % "provided",
  "org.eclipse.jetty"       %   "jetty-servlet"          % SparkJetty      % "provided",

  "com.codahale.metrics"    %   "metrics-core"           % CodaHaleMetrics % "provided",
  "com.codahale.metrics"    %   "metrics-json"           % CodaHaleMetrics % "provided",

  "ru.yandex.clickhouse"    %   "clickhouse-jdbc"        % clickhouse_jdbc % "provided",
  "org.apache.commons"      %   "commons-pool2"          % commons_pool
)

//multi-project for shading commons-pool2
lazy val assemblyJar = (project in file("spark-clickhouse-connector"))
  .enablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(libraryDependencies ++=deps)
  .settings(
    skip in publish := true,
    skip in publishM2 := true,
    assemblyOption in assembly ~= { _.copy(includeScala = false) },
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    assemblyJarName in assembly := s"${name.value}_2.11-${Versions.Spark}_${version.value}.jar",
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("org.apache.commons.**" -> "shade.io.clickhouse.apache.commons.@1").inAll
    )
  )

//hide shaded dependencies
lazy val connectorDistribution = (project in file("./"))
  .settings(commonSettings)
  .settings(
    packageBin in Compile := (assembly in (assemblyJar, Compile)).value,
    assembly := (assembly in assemblyJar).value

  )