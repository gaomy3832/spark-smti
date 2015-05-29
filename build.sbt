import AssemblyKeys._

assemblySettings

name := "smti"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>  {
  case PathList("META-INF", "ECLIPSEF.RSA", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "mailcap", xs @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", "beanutils",  xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", "collections",  xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging",  xs @ _*) => MergeStrategy.first
  case "plugin.properties" => MergeStrategy.first
  case x => old(x)
}}
