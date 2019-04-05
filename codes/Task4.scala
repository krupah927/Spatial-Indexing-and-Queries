package org.apache.spark.sql.simba.examples

/*
 * Part 2 task 4: Find nearby points and rank them
 * */

import java.io.File
import java.io.PrintWriter
import java.sql.Timestamp

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{count, month, lit}

object Task4 {
  case class TrajectoryPoint(tid: Int, oid: Int, lon: Double, lat: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("Usage: " + this.getClass.getSimpleName + " NumThreads NumPartitions InputTraj Radius OutputCSV")
      return
    }

    val simba = SimbaSession
      .builder()
      .master("local[" + args(0).toInt + "]")
      .appName("Project")
      .config("simba.index.partitions", args(1))  // Set the number of partitions via argument
      .config("simba.join.partitions", args(1))  // Set the number of partitions via argument
      .getOrCreate()

    import simba.implicits._

    val schema = ScalaReflection.schemaFor[TrajectoryPoint].dataType.asInstanceOf[StructType]

    // Self join with the same dataset will fail for distanceJoin, so we need 2 copies
    val points1 = simba.read
      .option("header", "false")
      .schema(schema)
      .csv(args(2))  // Input CSV
      .as[TrajectoryPoint]
      .filter(month($"time") >= 2 && month($"time") <= 6)  // Select points between Feb and Jun
      .cache()  // Cache in memory, hopefully it can run faster
    val points2 = simba.read
      .option("header", "false")
      .schema(schema)
      .csv(args(2))  // Input CSV
      .as[TrajectoryPoint]
      .filter(month($"time") >= 2 && month($"time") <= 6)  // Select points between Feb and Jun
      .cache()  // Cache in memory, hopefully it can run faster

    println("Points loaded")

    import simba.simbaImplicits._

    // One index is needed for scan, points1 is used for scan, points2 is used for index search
    points2.index(RTreeType, "rtPoints2", Array("lon", "lat"))  // R-Tree index over longitude and latitude

    println("Index done")

    val results = points1.as("ps1")
      .distanceJoin(points2.as("ps2"), Array("lon", "lat"), Array("lon", "lat"), args(3).toDouble)  // 'Self' join
      .select($"ps1.*")  // Select only attributes from points1
      .groupBy("tid", "oid", "lon", "lat", "time")  // Group by for count aggregation
      /* Count number of neighbors and minus 1. This removes the same point as its neighbor. */
      .agg(count("*").minus(lit(1)).as("cnt"))
      .orderBy($"cnt".desc)  // Sort in descending order
      .limit(20)  // Get top 20 results
      .collect()

    // Save results to file
    val pw = new PrintWriter(new File(args(4)))
    for (row <- results) {
      val tid = row(0).asInstanceOf[Int]
      val oid = row(1).asInstanceOf[Int]
      val lon = row(2).asInstanceOf[Double]
      val lat = row(3).asInstanceOf[Double]
      val time = row(4).asInstanceOf[Timestamp]
      val cnt = row(5).asInstanceOf[Long]
      // Remove the last 2 characters from timestamp, which represents the unused milliseconds part
      pw.write(tid + "," + oid + "," + lon + "," + lat + "," + time.toString().dropRight(2) + "," + cnt + "\n")
    }
    pw.close()

    println("Done")

    simba.stop()
  }
}
