package org.vkhristenko.insightrepo.apps

import org.vkhristenko.insightrepo.common._
import org.vkhristenko.insightrepo.login._

import java.io._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction

object MetricsProviderApp {
  def main(args: Array[String]) {
    val inputFileName = args(0)
    val outputF1 = args(1)
    val outputF2 = args(2)
    val outputF3 = args(3)
    val outputF4 = args(4)
    println(s"input = $inputFileName")

    println(s"Start...")
    // feature 4
    val manager = new LoginManager(outputF4)
    val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
    // NOTE: getLines returns an iterator, not the whole List!
    for (line <- Source.fromFile(inputFileName)(decoder).getLines) {
      val event = parseWithRE(line)
      manager.register(event)
    }

    println(s"Stop...")
  }
}
