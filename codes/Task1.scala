package org.apache.spark.sql.simba.examples

/*
 * Part 2 task 1: Find all restaurants in a rectangle
 * */

import java.io.File
import java.io.PrintWriter

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

object Task1 {
  case class POI(id: Long, desc: String, lon: Double, lat: Double)

  final val minLon = -339220.0
  final val maxLon = -309375.0
  final val minLat = 4444725.0
  final val maxLat = 4478070.0

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: " + this.getClass.getSimpleName + " NumPartitions InputPOI OutputCSV")
      return
    }

    val simba = SimbaSession
      .builder()
      .master("local[*]")
      .appName("Project")
      .config("simba.index.partitions", args(0))  // Set the number of partitions via argument
      .getOrCreate()

    import simba.implicits._

    val schema = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]
    val pois = simba.read
      .option("header", "false")
      .schema(schema)
      .csv(args(1))  // Input CSV
      .as[POI]

    println("POIs loaded")

    import simba.simbaImplicits._

    pois.index(RTreeType, "rtPoints", Array("lon", "lat"))  // R-Tree index over longitude and latitude

    println("Index done")

    val results = pois
      .range(Array("lon", "lat"), Array(minLon, minLat), Array(maxLon, maxLat))  // Search in rectangle
      .filter($"desc".contains("amenity=restaurant"))  // Filter by restaurant
      .orderBy("id")  // Order by ID
      .collect()

    // Save results to file
    val pw = new PrintWriter(new File(args(2)))
    for (row <- results) {
      val id = row(0).asInstanceOf[Long]
      val desc = row(1).asInstanceOf[String]
      val lon = row(2).asInstanceOf[Double]
      val lat = row(3).asInstanceOf[Double]
      if (desc.contains(","))
        pw.write(id + ",\"" + desc + "\"," + lon + "," + lat + "\n")
      else
        pw.write(id + "," + desc + "," + lon + "," + lat + "\n")
    }
    pw.close()

    println("Done")

    simba.stop()
  }
}
