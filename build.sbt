name := "insight-repo"

organization := "org.vkhristenko"

version := "0.0.1"

scalaVersion := "2.11.8"

spIgnoreProvided := true
sparkVersion := "2.1.0"
sparkComponents := Seq("sql")

resolvers += Resolver.mavenLocal
