name := "parking-violation-pipeline"

version := "0.1"

val scala211 = "2.11.12"

// Versions
val akkaVersion = "10.1.8"
val akkaStreamVersion = "2.5.23"
val alpakkaVersion = "1.0.4"
val catVersion = "1.3.1"
val hadoopGCConnectorVersion = "hadoop2-1.9.17"
val kafkaVersion = "2.3.0"
val logbackVersion = "1.2.3"
val okHttpVersion = "3.14.2"
val pureConfigVersion = "0.11.1"
val quartzVersion = "2.3.0"
val scalaLoggingVersion = "3.9.2"
val scalaTestVersion = "3.0.5"
val sparkVersion = "2.4.3"

// Libs
val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaVersion
val akkaMarshalling = "com.typesafe.akka" %% "akka-http-spray-json" % akkaVersion
val alpakka = "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaVersion
val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaStreamVersion

val catsEffect = "org.typelevel" %% "cats-effect" % catVersion
val catsCore = "org.typelevel" %% "cats-core" % catVersion
val hadoopGCConnector = "com.google.cloud.bigdataoss" % "gcs-connector" % hadoopGCConnectorVersion
val kafka = "org.apache.kafka" %% "kafka" % kafkaVersion

val logback = "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime
val okHttp = "com.squareup.okhttp3" % "okhttp" % okHttpVersion
val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
val quartz = "org.quartz-scheduler" % "quartz" % quartzVersion
val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
val scalaReflect = "org.scala-lang" % "scala-reflect" % scala211

val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"

// exclude
val excludeFromLog4j = "log4j" % "log4j" % "1.2.15" excludeAll(
  ExclusionRule(organization = "com.sun.jdmk"),
  ExclusionRule(organization = "com.sun.jmx"),
  ExclusionRule(organization = "javax.jms")
)

lazy val common = (project in file("common"))
  .settings(
    libraryDependencies ++= Seq(
      akkaMarshalling,
      catsCore,
      catsEffect,
      scalaLogging,
      scalaTest
    )
  ).settings(scalaVersion := scala211)

lazy val dataProvider = (project in file("data-provider"))
  .settings(
    libraryDependencies ++= Seq(
      akkaMarshalling,
      catsCore,
      catsEffect,
      excludeFromLog4j,
      hadoopGCConnector,
      logback,
      okHttp,
      pureConfig,
      scalaLogging,
      scalaTest
    )
  )
  .settings(
    scalaVersion := scala211,
    runSetting
  )
  .settings(
    mainClass in assembly := Some("pv.data.provider.DataProvider")
  ).dependsOn(common)

lazy val httpService = (project in file("http-service"))
  .settings(
    libraryDependencies ++= Seq(
      akkaMarshalling,
      kafka,
      alpakka,
      logback,
      pureConfig,
      scalaLogging,
      scalaTest
    )
  )
  .settings(
    scalaVersion := scala211,
    runSetting
  )
  .settings(
    mainClass in assembly := Some("pv.http.service.HttpService")
  ).dependsOn(common)

lazy val dataProcessor = (project in file("data-processor"))
  .settings(
    libraryDependencies ++= Seq(
      akkaMarshalling,
      akkaStream,
      excludeFromLog4j,
      hadoopGCConnector,
      kafka,
      logback,
      pureConfig,
      scalaLogging,
      scalaTest,
      sparkSql,
      sparkStreaming,
      sparkStreamingKafka,
      sparkSqlKafka
    )
  )
  .settings(
    mainClass in assembly := Some("pv.data.processing.DataProcessingApp")
  )
  .settings(commonSettings)
  .dependsOn(common)

lazy val aggregationService = (project in file("aggregation-service"))
  .settings(
    libraryDependencies ++= Seq(
      hadoopGCConnector,
      logback,
      pureConfig,
      quartz,
      scalaLogging,
      scalaTest,
      sparkSql
    )
  )
  .settings(commonSettings)
  .settings(Test / parallelExecution := false)
  .settings(
    mainClass in assembly := Some("pv.aggregation.AggregateApp")
  ).dependsOn(common)

  lazy val viewService = (project in file("view-service"))
    .settings(commonSettings)
    .settings(Test / parallelExecution := false)
    .settings(
      libraryDependencies ++= Seq(
        akkaHttp,
        akkaMarshalling,
        akkaStream,
        logback,
        pureConfig,
        quartz,
        scalaLogging,
        scalaTest,
        scalaReflect,
        sparkSql
      )
    )
    .settings(
    mainClass in assembly := Some("view.ViewApp")
  ).dependsOn(aggregationService % "test->test;compile->compile")
  

lazy val commonSettings = Seq(
  scalaVersion := scala211,
  runSetting,
  mergeStrategy,
  scalacOptions := Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-Ypartial-unification"
  )
)

lazy val runSetting = {
  run in Compile := Defaults
    .runTask(fullClasspath in Compile,
      mainClass in(Compile, run),
      runner in(Compile, run)
    ).evaluated
}

lazy val mergeStrategy = {
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}