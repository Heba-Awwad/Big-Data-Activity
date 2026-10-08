import org.apache.spark.sql.SparkSession

object PiActivity {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PiActivity")
      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext


    val n = 100000000


    val slices = 4


    val points = sc.parallelize(1 to n, slices)


    val xy = points.map { _ =>
      val x = 2 * math.random - 1
      val y = 2 * math.random - 1
      (x, y)
    }

    val start = System.nanoTime()

    val inside = xy
      .map { case (x, y) =>
        if (x * x + y * y <= 1.0) 1 else 0
      }
      .reduce(_ + _)

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
