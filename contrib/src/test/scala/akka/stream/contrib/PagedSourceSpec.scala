/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import org.scalatest.concurrent.ScalaFutures

import PagedSourceSpec._
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

object PagedSourceSpec {

  import scala.concurrent.ExecutionContext.Implicits.global

  case class MultiplesOfTwo(size: Option[Int] = None) {

    val itemsPerPage = 2

    def page(key: Int): Future[PagedSource.Page[Int, Int]] =
      Future {
        val indices = key * itemsPerPage until (key + 1) * itemsPerPage
        val filteredIndices = size match {
          case Some(sz) => indices.filter(_ < sz)
          case None     => indices
        }
        PagedSource.Page(filteredIndices.map(_ * 2), Some(key + 1))
      }
  }

  object IndexedStringPages {
    def page(key: Int): List[String] = key match {
      case 1 => List("a", "b", "c")
      case 2 => List("d", "e")
      case _ => Nil
    }
  }

  object LinkedIntPages {
    def page(key: String): (List[Int], String) = key match {
      case "first"  => (List(1, 2), "second")
      case "second" => (List(3, 4, 5), "")
      case _        => (List(6), "")
    }
  }

}

class PagedSourceSpecAutoFusingOn extends { val autoFusing = true } with PagedSourceSpec

class PagedSourceSpecAutoFusingOff extends { val autoFusing = false } with PagedSourceSpec

trait PagedSourceSpec extends BaseStreamSpec with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global

  "PagedSource - MultiplesOfTwo" should {
    "return the items in the proper order" in {
      val source = PagedSource(0)(MultiplesOfTwo().page(_))

      val result = source.take(3).runWith(Sink.seq)
      whenReady(result) { a =>
        a shouldBe List(0, 2, 4)
      }
    }

    "return not more items then available" in {
      val source = PagedSource(0)(MultiplesOfTwo(Some(4)).page(_))

      val result = source.take(10).runWith(Sink.seq)
      whenReady(result) { a =>
        a.length shouldBe 4
      }
    }
  }

  "PagedSource - IndexedStringPages" should {
    val source = PagedSource[String, Int](1)(i =>
      Future(PagedSource.Page(IndexedStringPages.page(i), Some(i + 1))))
    "return the items in the proper order" in {
      val result = source.take(4).runWith(Sink.seq)
      whenReady(result) { a =>
        a shouldBe Seq("a", "b", "c", "d")
      }
    }
    "close stream when received empty page" in {
      val result = source.runWith(Sink.seq)
      whenReady(result) { a =>
        a shouldBe Seq("a", "b", "c", "d", "e")
      }
    }
  }

  "PagedSource - LinkedIntPages" should {
    val source = PagedSource[Int, String]("first") { key =>
      val (items, next) = LinkedIntPages.page(key)
      Future(PagedSource.Page(items, if (next.isEmpty) None else Some(next)))
    }
    "return the items in the proper order" in {
      val result = source.take(4).runWith(Sink.seq)
      whenReady(result) { a =>
        a shouldBe Seq(1, 2, 3, 4)
      }
    }
    "close stream when received empty link" in {
      val result = source.runWith(Sink.seq)
      whenReady(result) { a =>
        a shouldBe Seq(1, 2, 3, 4, 5)
      }
    }
  }

}
