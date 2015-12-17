import com.google.common.hash._

/**
  * Stable Bloom Filter.
  * Similar interface to Google Guava's {@link com.google.common.hash.BloomFilter}.
  * Based on the document linked below, implementation leverages some of the Guava's code
  * from {@link com.google.common.hash.BloomFilter} and {@link com.google.common.hash.BloomFilterStrategies}.
  *
  * @see <a href="http://www.cs.ualberta.ca/~drafiei/papers/DupDet06Sigmod.pdf">
  *      Approximately Detecting Duplicates for Streaming Data using Stable Bloom Filters, by
  *      Fan Deng and Davood Rafiei, University of Alberta</a>
  *
  * Ported to scala from java implementation https://github.com/ru2nuts/stable_bloom_filter/blob/master/src/main/java/com/visiblemeasures/common/collections/StableBloomFilter.java
  */
class StableBloomFilter[T](numCells: Int,
    private val numHashFunctions: Int,
    private val numDecrementCells: Int,
    private val maxVal: Byte,
    private val funnel: Funnel[T]) {

  import StableBloomFilter._

  private val cells = new Array[Byte](numCells)

  private val strategy = Murmur128_Mitz_32_Strategy

  def mightContain(`object`: T): Boolean = {
    strategy.mightContain(`object`, funnel, numHashFunctions, cells)
  }

  def put(`object`: T): Boolean = {
    decrementCells()
    strategy.put(`object`, funnel, numHashFunctions, maxVal, cells)
  }

  private def decrementCells() {
    val min = 0
    val max = cells.length - 1
    var decrementPos = min + (Math.random() * ((max - min) + 1)).toInt
    for (i <- 0 until numDecrementCells) {
      if (decrementPos >= cells.length) {
        decrementPos = 0
      }
      if (cells(decrementPos) > 0) {
        cells(decrementPos) = (cells(decrementPos) - 1).toByte
      }
      decrementPos += 1
    }
  }
}

object StableBloomFilter {

  object Murmur128_Mitz_32_Strategy {

    def put[T](`object`: T,
        funnel: Funnel[T],
        numHashFunctions: Int,
        maxVal: Byte,
        cells: Array[Byte]): Boolean = {
      val hash64 = Hashing.murmur3_128().newHasher().putObject(`object`, funnel)
          .hash()
          .asLong()
      val hash1 = hash64.toInt
      val hash2 = (hash64 >>> 32).toInt
      var bitsChanged = false
      var i = 1
      while (i <= numHashFunctions) {
        var nextHash = hash1 + i * hash2
        if (nextHash < 0) {
          nextHash = ~nextHash
        }
        val pos = nextHash % cells.length
        bitsChanged |= (cells(pos) != maxVal)
        cells(pos) = maxVal
        i += 1
      }
      bitsChanged
    }

    def mightContain[T](`object`: T,
        funnel: Funnel[_ >: T],
        numHashFunctions: Int,
        cells: Array[Byte]): Boolean = {
      val hash64 = Hashing.murmur3_128().newHasher().putObject(`object`, funnel)
          .hash()
          .asLong()
      val hash1 = hash64.toInt
      val hash2 = (hash64 >>> 32).toInt
      var i = 1
      while (i <= numHashFunctions) {
        var nextHash = hash1 + i * hash2
        if (nextHash < 0) {
          nextHash = ~nextHash
        }
        val pos = nextHash % cells.length
        if (cells(pos) == 0) {
          return false
        }
        i += 1
      }
      true
    }
  }
}
