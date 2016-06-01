/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink

class AlphabeticFramingSpecAutoFusingOn extends { val autoFusing = true } with AlphabeticFramingSpec
class AlphabeticFramingSpecAutoFusingOff extends { val autoFusing = false } with AlphabeticFramingSpec

trait AlphabeticFramingSpec extends BaseStreamSpec {
  import AlphabeticFraming._

  "AlphabeticFraming" should {
    "emit frames either fully alphabetic or fully non-alphabetic" in {
      Source.single("This is  a test \nfor\nalphabetic-framing.")
        .via(AlphabeticFraming())
        .runWith(TestSink.probe)
        .request(99)
        .expectNext(Frame("This", true))
        .expectNext(Frame(" ", false))
        .expectNext(Frame("is", true))
        .expectNext(Frame("  ", false))
        .expectNext(Frame("a", true))
        .expectNext(Frame(" ", false))
        .expectNext(Frame("test", true))
        .expectNext(Frame(" \n", false))
        .expectNext(Frame("for", true))
        .expectNext(Frame("\n", false))
        .expectNext(Frame("alphabetic", true))
        .expectNext(Frame("-", false))
        .expectNext(Frame("framing", true))
        .expectNext(Frame(".", false))
        .expectComplete()
    }
  }
}
