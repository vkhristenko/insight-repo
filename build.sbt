name := "insight-repo"

organization := "org.vkhristenko"

version := "0.0.1"

scalaVersion := "2.11.8"

spIgnoreProvided := true
sparkVersion := "2.1.0"
sparkComponents := Seq("sql")

resolvers += Resolver.mavenLocal
/*libraryDependencies += "org.diana-hep" % "spark-root_2.11" % "0.1.10"
libraryDependencies += "org.diana-hep" % "histogrammar-sparksql_2.11" % "1.0.3"
libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.2.11"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.12"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.12"
libraryDependencies += "org.sameersingh.scalaplot" % "scalaplot" % "0.0.4"
*/
