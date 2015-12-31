import java.io.InputStream

import com.google.common.base.Charsets
import com.google.common.hash.Funnels
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.mutable


class StableBloomFilterTest extends WordSpec with Matchers with BeforeAndAfter  {

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



    "error rates are within acceptable boundaries" should {

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




    "count distinct lines" ignore {

      var actualDistinct = 0

      var fileCount = 0
      val linesToRead = 1000 * 1000 / 10
      val lineSet = mutable.Set[String]()

      for (file <- new java.io.File("c:\\temp\\real_data_files\\").listFiles().filter(f => f.getName.contains("20151202_11"))) {
        fileCount += 1

        for (line0 <- scala.io.Source.fromFile(file).getLines().take(linesToRead)) {

          //val tsMs = line0.substring(0, 13)
          //val tsSec = line0.substring(0, 10)
          //val ts10Sec = line0.substring(0, 9)

          //val time = new java.util.Date(tsMs.toLong)

          //time.setSeconds(0)
          //time.setMinutes(0)
          // tens of minutes
          //time.setMinutes(time.getMinutes - time.getMinutes % 10)

          //val line = time.toString + line0.substring(13)
          //val line = ts10Sec + line0.substring(13)
          //val line = line0
          // ignore time
          val line = line0.substring(13)

          if (!lineSet.contains(line)) {
            lineSet += line
            actualDistinct += 1
          }
        }

      }

      "output stats" in {
        val totalLinesRead = linesToRead*fileCount
        println(s"fileCount   = $fileCount" )
        println(s"totalLinesRead   = $totalLinesRead" )
        println(s"actualDistinct %   = ${(actualDistinct.toFloat/totalLinesRead)*100} %" )
        totalLinesRead should be > 1
      }
    }

  }

}
