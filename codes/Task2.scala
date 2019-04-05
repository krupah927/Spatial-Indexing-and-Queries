package org.apache.spark.sql.simba.examples

/*
 * Part 2 task 2: Find popular places.
 * */

import java.io.File
import java.io.PrintWriter
import java.sql.Timestamp;

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._

object Task2 {
  case class TrajectoryPoint(tid: Int, oid: Int, lon: Double, lat: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: " + this.getClass.getSimpleName + " NumPartitions InputTraj OutputCSV")
      return
    }

    val simba = SimbaSession
      .builder()
      .master("local[*]")
      .appName("Project")
      .config("simba.index.partitions", args(0))  // Set the number of partitions via argument
      .getOrCreate()

    import simba.implicits._

    val schema = ScalaReflection.schemaFor[TrajectoryPoint].dataType.asInstanceOf[StructType]
    val points = simba.read
      .option("header", "false")
      .schema(schema)
      .csv(args(1))  // Input CSV
      .as[TrajectoryPoint]

    println("Points loaded")

    import simba.simbaImplicits._

    points.index(RTreeType, "rtPoints", Array("lon", "lat"))  // R-Tree index over longitude and latitude

    println("Index done")

    val tmpResults = points
      .circleRange(Array("lon", "lat"), Array(-322357.0, 4463408.0), 2000.0)  // Search in circle within 2000 m
      .withColumn("weekday", date_format(col("time"), "EE"))  // Get weekday
      .withColumn("date_hour", date_format(col("time"), "yyyy-MM-dd HH"))  // Get hour of day
      .filter($"weekday" =!= "Sat" && $"weekday" =!= "Sun")  // Filter by weekdays (not weekends)

    tmpResults.createOrReplaceTempView("tmpResults")  // Create a view for query

    // Sub-query for counting distinct objects in each hour of any day
    val subQuery = "SELECT COUNT(DISTINCT tid) AS cnt, hour(date_hour) AS hour FROM tmpResults GROUP BY date_hour"
    // Compute averge number of objects in each hour across all days
    val results = simba.sql("SELECT hour, CAST(ROUND(AVG(cnt)) AS INT) FROM (" +
      subQuery +
      ") GROUP BY hour ORDER BY hour")
        .collect()

    // Save results to file
    val pw = new PrintWriter(new File(args(2)))
    for (row <- results) {
      val hour = row(0).asInstanceOf[Int]
      val num = row(1).asInstanceOf[Int]
      // Make hours 2 digits
      if (hour < 10)
        pw.write(s"0$hour,$num\n")
      else
        pw.write(s"$hour,$num\n")
    }
    pw.close()

    println("Done")

    simba.stop()
  }
}
