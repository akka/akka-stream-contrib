/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import java.time.Clock
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.stream.contrib.LatencyTimer.TimedResult
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.{ActorAttributes, Supervision}
import org.scalamock.scalatest.MockFactory
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{GivenWhenThen, OptionValues}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

import akka.stream.contrib.Implicits._

class LatencyTimerSpec extends AnyFeatureSpec with GivenWhenThen with Matchers with MockFactory with OptionValues {

  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  /**
   *
   */
  Scenario("LatencyTimer measures time correct and continues with the wrapped flow") {
    val clockMock = mock[Clock]
    val baseInstant = Clock.systemDefaultZone().instant()
    var timerResult: FiniteDuration = null

    val TimePassed = 100.millis

    Given(s"a Flow stage that 'waits' for $TimePassed")
    val testFlow = Flow.fromFunction[String, String] { s =>
      s"hello $s"
    }
    val measuredFlow = LatencyTimer.createGraph(
      testFlow,
      Sink.foreach[TimedResult[String]](x => timerResult = x.measuredTime)
    )(clockMock)

    (clockMock.instant _).expects().returns(baseInstant)
    (clockMock.instant _).expects().returns(baseInstant.plus(TimePassed.length, ChronoUnit.MILLIS))

    When("running the stream with one element")
    val (sourceProbe, materializedValue) = TestSource.probe[String].via(measuredFlow).toMat(Sink.head)(Keep.both).run()
    sourceProbe.sendNext("timer")
    sourceProbe.sendComplete()
    Await.result(materializedValue, 1.seconds)

    Then(s"the measured time is $TimePassed")
    timerResult should equal(TimePassed)

    And("the wrapped flow materializes correct")
    materializedValue.value.value.get should equal("hello timer")
  }

  /**
   *
   */
  Scenario("LatencyTimer handles diverting Flows") {
    val clockMock = mock[Clock]
    val baseInstant = Clock.systemDefaultZone().instant()
    var timerResult: FiniteDuration = null

    val TimePassedFirst = 100.millis
    val TimePassedSecondDiverted = 10.millis
    val TimePassedThird = 50.millis

    Given("a Integer-Flow that diverts for element == 2")
    val divertingTestFlow = Flow
      .fromFunction[Int, Int] { i =>
        i
      }
      .divertTo(Sink.ignore, i => i == 2)
    val measuredFlow = LatencyTimer.createGraph(
      divertingTestFlow,
      Sink.foreach[TimedResult[Int]](x => timerResult = x.measuredTime)
    )(clockMock)

    // first element
    (clockMock.instant _).expects().returns(baseInstant)
    (clockMock.instant _).expects().returns(baseInstant.plus(TimePassedFirst.length, ChronoUnit.MILLIS))
    // second element
    (clockMock.instant _)
      .expects()
      .returns(baseInstant) // (only started because the stop clock is never called because of diverted element)
    // third element
    (clockMock.instant _).expects().returns(baseInstant)
    (clockMock.instant _).expects().returns(baseInstant.plus(TimePassedThird.length, ChronoUnit.MILLIS))

    When("running the stream with one element, measuring the time")
    val (sourceProbe, materializedValue) = TestSource.probe[Int].via(measuredFlow).toMat(Sink.last)(Keep.both).run()
    sourceProbe.sendNext(1)
    sourceProbe.sendNext(2)
    sourceProbe.sendNext(3)
    sourceProbe.sendComplete()
    Await.result(materializedValue, 1.second)

    Then(s"the measured time is $TimePassedThird, which is the last element sent")
    timerResult should equal(TimePassedThird)

    And("the wrapped flow materializes correct")
    materializedValue.value.value.get should equal(3)
  }

  /**
   * In the DSL we cannot mock out the Clock instance, hence we test DSL separatly
   */
  Scenario("LatencyTimer DSL works as expected") {
    When("running a stream with a flow-stage called by the DSL")
    val measuredFlow = Flow
      .fromFunction[String, String](s => s"hello $s")
      .measureLatency(Sink.ignore)
    val (sourceProbe, materializedValue) = TestSource
      .probe[String]
      .via(measuredFlow)
      .toMat(Sink.head)(Keep.both)
      .run()
    sourceProbe.sendNext("timer")
    sourceProbe.sendComplete()
    Await.result(materializedValue, 1.second)

    Then("the wrapped flow materializes correct")
    materializedValue.value.value.get should equal("hello timer")
  }

  Scenario("LatencyTimer handles errors") {
    val clockMock = mock[Clock]
    val baseInstant = Clock.systemDefaultZone().instant()
    var timerResult: FiniteDuration = null

    val TimePassedFirst = 100.millis
    val TimePassedSecondDiverted = 10.millis
    val TimePassedThird = 50.millis

    Given("a resume restart policy")
    val decider: Supervision.Decider = _ => Supervision.Resume

    // first element
    (clockMock.instant _).expects().returns(baseInstant)
    (clockMock.instant _).expects().returns(baseInstant.plus(TimePassedFirst.length, ChronoUnit.MILLIS))
    // second element (only started because the stop clock is never called due to resume)
    (clockMock.instant _).expects().returns(baseInstant)
    // third element
    (clockMock.instant _).expects().returns(baseInstant)
    (clockMock.instant _).expects().returns(baseInstant.plus(TimePassedThird.length, ChronoUnit.MILLIS))

    When("running a stream that is throwing and error")
    val flow = Flow.fromFunction[Int, Int](i => if (i == 2) throw new RuntimeException("foobar") else i)
    val measuredFlow =
      LatencyTimer.createGraph(flow, Sink.foreach[TimedResult[Int]](x => timerResult = x.measuredTime))(clockMock)
    val (sourceProbe, materializedValue) = TestSource
      .probe[Int]
      .via(measuredFlow)
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .toMat(Sink.last)(Keep.both)
      .run()
    sourceProbe.sendNext(1)
    sourceProbe.sendNext(2)
    sourceProbe.sendNext(3)
    sourceProbe.sendComplete()
    Await.result(materializedValue, 1.second)

    Then("the wrapped flow materializes correct")
    materializedValue.value.value.get should equal(3)

    And(s"the measured time is $TimePassedThird, which is the element sent after resuming")
    timerResult should equal(TimePassedThird)
  }
}
