package org.apache.spark.sql.simba.examples

/*
 * Work on a sample of trajectories dataset to compute MBRs for each partition, and output the MBRs to file.
 * */

import java.io.File
import java.io.PrintWriter

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

object Trajectories {
  case class TrajectoryPoint(tid: Int, oid: Int, lon: Double, lat: Double, time: String)
  case class MBR(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: " + this.getClass.getSimpleName + " NumPartitions InputTraj OutputCSV")
      return
    }

    val simba = SimbaSession
      .builder()
      .master("local[4]")
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

    // Resave the points to file for plotting
    val pw = new PrintWriter(new File(args(2)))
    for (p <- points.collect()) {
      val lon = p.lon
      val lat = p.lat
      pw.write(s"$lon,$lat\n")
    }

    import simba.simbaImplicits._

    points.index(RTreeType, "rtPoints", Array("lon", "lat"))  // R-Tree index over longitude and latitude

    println("Index done")

    // Get the min and max longitude and latitude from each partition, and construct a MBR
    def getMBR(pts : Iterator[TrajectoryPoint]) : Iterator[MBR] = {
      var minLon = Double.MaxValue
      var minLat = Double.MaxValue
      var maxLon = Double.MinValue
      var maxLat = Double.MinValue
      while(pts.hasNext) {
        val p = pts.next()
        if (p.lat > maxLat)
          maxLat = p.lat
        if (p.lon > maxLon)
          maxLon = p.lon
        if (p.lat < minLat)
          minLat = p.lat
        if (p.lon < minLon)
          minLon = p.lon
      }
      val mbr = MBR(minLon, minLat, maxLon, maxLat)
      return Iterator(mbr)
    }

    // Compute MBRs for each partition and save MBRs to file for plotting
    val results = points.mapPartitions(getMBR).collect
    for (mbr <- results) {
      // Position of the MBR
      val x = mbr.minLon
      val y = mbr.minLat
      // Size of the MBR
      val w = mbr.maxLon - mbr.minLon
      val h = mbr.maxLat - mbr.minLat
      pw.write(s"$x,$y,$w,$h\n")
    }

    pw.close()

    println("Done")

    simba.stop()
  }
}
