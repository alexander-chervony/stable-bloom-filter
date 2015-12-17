import java.io.InputStream

import com.google.common.base.Charsets
import com.google.common.hash.Funnels
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.mutable


class StableBloomFilterTest extends WordSpec with Matchers with BeforeAndAfter  {

//  private var sbf: StableBloomFilter[CharSequence] = null

//  before {
//    sbf = new StableBloomFilter[CharSequence](50, 10, 5, Funnels.stringFunnel(Charsets.UTF_8))
//  }

  "StableBloomFilter" when {

    "several items put in filter" should {
      // actually with 0 for numDecrementCells param its just bloom filter rather than stable bloom filter
      val sbf = new StableBloomFilter[CharSequence](50, 10, 0, 3, Funnels.stringFunnel(Charsets.UTF_8))
      for (i <- 1 to 9) {
        sbf.put("test" + i)
      }
      "contain existent" in {
        for (j <- 1 to 9) {
          sbf.mightContain("test" + j) shouldBe true
        }
      }
      // this check actually stably works only due to filter input params
      "and not contain false positive" in {
        sbf.mightContain("test10") shouldBe false
      }
    }



    "20 mln lines file processed" should {

      val sbf = new StableBloomFilter[CharSequence](64*1024*1024/100, 10, 10, 1, Funnels.stringFunnel(Charsets.UTF_8))

      var actualDistinct = 0

      val linesToRead = 20000
      val lineSet = mutable.Set[String]()
      var fpCount = 0
      var fnCount = 0
      var total = 0

      val stream : InputStream = getClass.getResourceAsStream("/filterInput.txt")

      for(line <- scala.io.Source.fromInputStream( stream ).getLines().take(linesToRead)){

        val reallyDistinct = !lineSet.contains(line)
        if (reallyDistinct) {
          lineSet += line
          actualDistinct += 1
        }

        if (sbf.put(line)) {
          // filter considers the item distinct
          if (!reallyDistinct) {
            fnCount += 1
          }
        } else {
          // filter detected duplicate
          if (reallyDistinct){
            fpCount += 1
          }
        }

        total += 1
      }

      println(s"actualDistinct   = $actualDistinct")

      "FP rate must be less than 1%" in {
        // FP causes actual distincts to be missing in output stream
        //val fp = ((actualDistinct - computedDistinct).toFloat/actualDistinct) * 100
        val fp = (fpCount.toFloat/actualDistinct) * 100
        println(s"fpCount = $fpCount")
        println(s"FP % = $fp")
        fp should be < 1f
      }

      "FN rate must be less than 1%" in {
        // FN causes duplicates in output stream
        val fn = (fnCount.toFloat/actualDistinct) * 100
        println(s"fnCount = $fnCount")
        println(s"FN % = $fn")
        fn should be < 1f
      }
    }

  }

}
