package akka.stream.contrib

import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}

import scala.concurrent.Future
import scala.concurrent.duration._

class DebounceSpecAutoFusingOn extends { val autoFusing = true } with DebounceSpec
class DebounceSpecAutoFusingOff extends { val autoFusing = false } with DebounceSpec


trait DebounceSpec extends BaseStreamSpec {
  "Debounce" should {
    "debounce" in {
      val flow = Flow[Int].via(Debounce(2.seconds))
      val (pub, sub) = TestSource.probe[Int].via(flow).toMat(TestSink.probe[Int])(Keep.both).run()
      pub.sendNext(5)
      pub.sendNext(4)
      pub.sendNext(3)
      pub.sendNext(4)
      pub.sendNext(5)
      pub.sendNext(5)
      pub.sendNext(4)
      pub.sendNext(3)
      pub.sendNext(4)

      pub.sendComplete()
      sub.request(3)
      sub.expectNext(5, 3, 4)
      sub.expectComplete()
    }

    "not push prematurely" in {
      val flow = Flow[Int].via(Debounce(3.seconds))
      val (pub, sub) = TestSource.probe[Int].via(flow).toMat(TestSink.probe[Int])(Keep.both).run()
      pub.sendNext(5)
      pub.sendNext(4)
      pub.sendNext(3)
      pub.sendNext(4)
      pub.sendNext(5)
      pub.sendNext(5)
      pub.sendNext(4)
      pub.sendNext(3)
      pub.sendNext(4)

      pub.sendComplete()
      sub.request(3)
      sub.expectNoMsg(2.seconds)
      sub.expectNext(5, 3, 4)
      sub.expectComplete()

    }

    "push in the right amount of time" in {
      val flow = Flow[Int].via(Debounce(3.seconds))
      val (pub, sub) = TestSource.probe[Int].via(flow).toMat(TestSink.probe[Int])(Keep.both).run()
      pub.sendNext(5)
      sub.request(1)
      sub.expectNext(4.seconds, 5)
    }

    "return the last element for a given aggregation function" in {
      val flow = Flow[Int].via(Debounce(2.seconds, x => x % 2))
      val (pub, sub) = TestSource.probe[Int].via(flow).toMat(TestSink.probe[Int])(Keep.both).run()
      pub.sendNext(5)
      pub.sendNext(4)
      pub.sendNext(3)
      pub.sendNext(2)
      pub.sendNext(1)
      pub.sendNext(3)
      sub.request(2)
      sub.expectNext(2, 3)
    }

    "allow repeat elements for a given aggregation function after a period of time" in {
      implicit val ec = system.dispatcher

      val flow = Flow[Int].via(Debounce(500.millis, x => x % 2))
      val (pub, sub) = TestSource.probe[Int].via(flow).toMat(TestSink.probe[Int])(Keep.both).run()
      pub.sendNext(5)
      pub.sendNext(4)
      pub.sendNext(3)
      akka.pattern.after(1500.millis, using = system.scheduler)(Future {
        pub.sendNext(2)
        pub.sendNext(1)
        pub.sendNext(3)
      })
      sub.request(4)
      sub.expectNext(4, 3, 2, 3)
    }
  }
}
