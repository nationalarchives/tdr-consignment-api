package uk.gov.nationalarchives.tdr.api.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}
import uk.gov.nationalarchives.tdr.api.utils.NaturalSorting.{ArrayOrdering, natural}

class NaturalSortingSpec extends AnyFlatSpec {

  val list1: Seq[String] = List("test file1", "test file11", "test file2", "test file3", "test file31", "test file4", "test file21", "test file23", "test file22")
  val expectedList1: Seq[String] = List("test file1", "test file2", "test file3", "test file4", "test file11", "test file21", "test file22", "test file23", "test file31")
  val list2: Seq[String] = List("test file", "test file 11", "test file 2", "test file 3", "test file 31", "test file 4", "test file 21", "test file 23", "test file 22")
  val expectedList2: Seq[String] = List("test file", "test file 2", "test file 3", "test file 4", "test file 11", "test file 21", "test file 22", "test file 23", "test file 31")
  val list3: Seq[String] = List("test1 file", "test11 file", "test2 file", "test5 file", "test12 file", "test21 file", "test file22", "test31 file", "test3 file")
  val expectedList3: Seq[String] = List("test1 file", "test2 file", "test3 file", "test5 file", "test11 file", "test12 file", "test21 file", "test31 file", "test file22")
  val list4: Seq[String] = List("test1.1 file", "test1.2 file", "test22 file", "test2.1 file", "test2.0 file", "test3.3 file", "test3.1 file", "test file", "test file22")
  val expectedList4: Seq[String] = List("test1.1 file", "test1.2 file", "test2.0 file", "test2.1 file", "test3.1 file", "test3.3 file", "test22 file", "test file", "test file22")
  val list5: Seq[String] = List("test file", "abc file", "bcd file", "123 file", "mno file", "abb file", "baa file", "edr file", "tre file22")
  val expectedList5: Seq[String] = List("123 file", "abb file", "abc file", "baa file", "bcd file", "edr file", "mno file", "test file", "tre file22")

  val input: Seq[(Int, Seq[String], Seq[String])] = List(
    (1, list1, expectedList1),
    (2, list2, expectedList2),
    (3, list3, expectedList3),
    (4, list4, expectedList4),
    (5, list5, expectedList5)
  )

  input.foreach { input =>
    "natural" should "sort a list by natural sorting order " + input._1 in {

      input._2.sortBy(natural) should equal(input._3)
    }
  }
}
