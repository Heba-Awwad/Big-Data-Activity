import org.apache.spark.sql.SparkSession

object PiActivity {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PiActivity")
      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext

    val n = 10000000

    val slices = 4

    val points = sc.parallelize(1 to n, slices)


    val xy = points.map { _ =>
      val x = 2 * math.random - 1
      val y = 2 * math.random - 1
      (x, y)
    }

    val start = System.nanoTime()

    val inside = xy
      .filter{ case (x, y) => x * x + y * y <= 1.0
      }.count

// val inside = xy
//  .map { case (x, y) =>
//    if (x * x + y * y <= 1.0) 1 else 0
//  }
//  .reduce(_ + _)

    val end = System.nanoTime()

    val timeSeconds = (end - start) / 1e9


    val piEstimate = 4.0 * inside / n


    val error = math.abs(piEstimate - math.Pi)

    println(f"n = $n%,d")
    println(f"partitions = $slices")
    println(f"points inside circle = $inside%,d")
    println(f"pi ~ $piEstimate%.6f")
    println(f"error = $error%.6f")
    println(f"time = $timeSeconds%.4f seconds")
    println(s"Actual partitions = ${xy.getNumPartitions}")


    spark.stop()
  }
}
// Overall Conclusion:
//This experiment demonstrated the relationship between data size, partitioning, parallel execution, accuracy, and performance in Apache Spark.
// Increasing the number of samples improved the accuracy of the Monte Carlo estimation of π, but increased computational time.
// Changing the number of partitions demonstrated that partitioning provides opportunities for parallel execution,
// but excessive partitioning can introduce task-management overhead, particularly in a local environment.
// The Spark UI provided a practical view of how Spark translates RDD operations into stages and tasks.
// Finally, rewriting filter + count as map + reduce demonstrated that the same computation can be expressed using different Spark transformations and actions while maintaining a similar estimation of π.