package sim

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import scala.math.sqrt

// Custom partitioner for key-based partitioning on userId
class UserIdKeyPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key match {
    case (userId: Int, _) => math.abs(userId.hashCode()) % numPartitions
    case userId: Int      => math.abs(userId.hashCode()) % numPartitions
    case _ =>
      throw new IllegalArgumentException(s"Invalid key format: $key")
  }
}

object SparseItemSim {
  def main(args: Array[String]): Unit = {
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)

    if (args.length != 3) {
      logger.error("Usage: sim.SparseItemSim <user_song_matrix> <output_dir> <topK>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("TopKItemSimilarity")
    val sc = new SparkContext(conf)

    val topK = args(2).toInt
    val minCoRatings = 5
    val numPartitions = 10000
    // val numPartitions = sc.defaultParallelism * 4

    // Step 1: Load user-song matrix
    val userSongMatrix: RDD[(Int, (Int, Double))] = sc.textFile(args(0))
      .map { line =>
        val Array(user, song, playcount) = line.split(",").map(_.trim)
        (user.toInt, (song.toInt, playcount.toDouble))
      }
      .cache()

    // Step 2: Compute user average ratings
    val userAverageRatings: RDD[(Int, Double)] = userSongMatrix
      .mapValues { case (_, rating) => (rating, 1) }
      .reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      .mapValues { case (sum, count) => sum / count }

    // Step 3: Adjust ratings by subtracting user means
    val userSongAdjustedRatings: RDD[(Int, (Int, Double))] = userSongMatrix
      .join(userAverageRatings)
      .mapValues { case ((songId, rating), userAvg) =>
        (songId, rating - userAvg)
      }
      .cache()

    // Step 4: Create horizontal partition (by userId)
    val horizontalPartition = userSongAdjustedRatings
      .partitionBy(new org.apache.spark.HashPartitioner(numPartitions))
      .cache()

    // Step 5: Create vertical partition (by (userId, songId), partitioned by userId)
    val verticalPartition = userSongAdjustedRatings
      .map { case (userId, (songId, rating)) =>
        ((userId, songId), rating)
      }
      .partitionBy(new UserIdKeyPartitioner(numPartitions))
      .cache()

    // Step 6: Generate co-occurrence pairs using both partitions, avoiding duplicates
    val itemPairs = horizontalPartition
      .map { case (userId, (songId, rating)) =>
        // Key is userId, Value is (songId, rating)
        (userId, (songId, rating))
      }
      .join(
        verticalPartition.map { case ((userId, songId), rating) =>
          // Key is userId, Value is (songId, rating)
          (userId, (songId, rating))
        }
      )
      // Filter out pairs where song1 >= song2 to avoid duplicates and self-pairs
      .filter { case (_, ((song1, _), (song2, _))) =>
        song1 < song2
      }
      // Map to ((song1, song2), (dotProduct, 1))
      .map { case (_, ((song1, rating1), (song2, rating2))) =>
        ((song1, song2), (rating1 * rating2, 1))
      }
      // Sum dot products and counts across all users
      .reduceByKey { case ((dotProd1, count1), (dotProd2, count2)) =>
        (dotProd1 + dotProd2, count1 + count2)
      }
      // Filter out pairs with insufficient co-ratings
      .filter { case (_, (_, count)) => count >= minCoRatings }
      .cache()

    // Step 7: Compute norms for each song
    val songNorms = userSongAdjustedRatings
      .map { case (_, (songId, rating)) => (songId, rating * rating) }
      .reduceByKey(_ + _)
      .cache()

    // Step 8: Calculate similarities
    val itemSimilarities = itemPairs
      .map { case ((song1, song2), (dotProduct, count)) =>
        (song1, (song2, dotProduct))
      }
      .join(songNorms)
      .map { case (song1, ((song2, dotProduct), norm1)) =>
        (song2, (song1, dotProduct, norm1))
      }
      .join(songNorms)
      .map { case (song2, ((song1, dotProduct, norm1), norm2)) =>
        val denominator = sqrt(norm1) * sqrt(norm2)
        val similarity = if (denominator != 0.0) dotProduct / denominator else 0.0
        ((song1, song2), similarity)
      }
      .filter { case (_, similarity) => !similarity.isNaN && similarity > 0.0 }
      .cache()

    // Step 9: Get top K similar songs
    val topKSimilarSongs = itemSimilarities
      .flatMap { case ((song1, song2), similarity) =>
        Seq(
          (song1, (song2, similarity)),
          (song2, (song1, similarity))
        )
      }
      .groupByKey()
      .mapValues { similarities =>
        similarities.toList
          .filter { case (_, sim) => sim > 0.0 }
          .sortBy(-_._2)
          .take(topK)
      }

    // Step 10: Format and save results
    val formattedOutput = topKSimilarSongs.map { case (song, similarSongs) =>
      val similarSongsStr = similarSongs
        .map { case (simSong, simScore) => f"($simSong,${simScore}%.4f)" }
        .mkString(", ")
      s"$song: ($similarSongsStr)"
    }

    // Save results
    formattedOutput.saveAsTextFile(args(1))

    // Clean up
    userSongMatrix.unpersist()
    userSongAdjustedRatings.unpersist()
    horizontalPartition.unpersist()
    verticalPartition.unpersist()
    itemPairs.unpersist()
    songNorms.unpersist()
    itemSimilarities.unpersist()

    sc.stop()
  }
}
