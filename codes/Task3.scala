package org.apache.spark.sql.simba.examples

/*
 * Part 2 task 3: Start and stop points
 * */

import java.io.File
import java.io.PrintWriter
import java.sql.Timestamp

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.{HashMapType, RTreeType}
import org.apache.spark.sql.types.{DateType, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._

object Task3 {

  case class TrajectoryPoint(tid: Int, oid: Int, lon: Double, lat: Double, time: Timestamp)

  final val minLon = -339220.0
  final val maxLon = -309375.0
  final val minLat = 4444725.0
  final val maxLat = 4478070.0
  final val midLon = (minLon + maxLon) / 2
  final val midLat = (minLat + maxLat) / 2

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: " + this.getClass.getSimpleName + " NumPartitions InputTraj OutputFile")
      return
    }

    val simba = SimbaSession
      .builder()
      .master("local[*]")
      .appName("Project")
      .config("simba.index.partitions", args(0)) // Set the number of partitions via argument
      .getOrCreate()

    import simba.implicits._

    val schema = ScalaReflection.schemaFor[TrajectoryPoint].dataType.asInstanceOf[StructType]
    val points = simba.read
      .option("header", "false")
      .schema(schema)
      .csv(args(1)) // Input CSV
      .as[TrajectoryPoint]

    val newpoint = points.withColumn("dateColumn", points("time").cast(DateType))

    newpoint.createOrReplaceTempView("points")

    //select starting points of tid
    val dfmin=simba.sql("SELECT tid, oid, MIN(time) AS time FROM points GROUP BY dateColumn, tid, oid")
    //select end points
    val dfmax=simba.sql("SELECT tid, oid, MAX(time) AS time FROM points GROUP BY dateColumn, tid, oid")

    println("joined")
    val combined=dfmin.select("*").union(dfmax.select("*")).toDF()
    //combine both dataframes

    val datajoined=combined.join(newpoint,usingColumns = Seq("time","tid","oid"))
    //join to get lat long

    //remove duplicates if any
    val datatemp=datajoined.select("tid","time","lat","lon","dateColumn","oid").distinct()

    import simba.simbaImplicits._

    //build r tree index
    val dataIndexed=datatemp.index(RTreeType, "idxPoints", Array("lon", "lat"))

    val dataInRect=dataIndexed.range(Array("lon", "lat"), Array(minLon, minLat), Array(maxLon, maxLat))
    //points.createOrReplaceTempView("dataIndexed")  // Create a view for query

    val finalselectdata=dataInRect.groupBy("tid","dateColumn","oid").count().filter("count>1")
    val total=finalselectdata.count()
    //    finalselectdata.show(10)

    import simba.simbaImplicits._

    // build rtree on finalized points
    val finalPoints=dataInRect.index(RTreeType, "idfinalPoints", Array("lon", "lat"))

    val quadrantTL = finalPoints
      .range(Array("lon", "lat"), Array(minLon, midLat), Array(midLon, maxLat))  // Search in top left rectangle
    // Top right

    val topleft=quadrantTL.groupBy("tid","dateColumn","oid").count().filter("count>1")
    //select those which have count >1 that is start and end in same quadrant

    val quadrantTR = finalPoints
      .range(Array("lon", "lat"), Array(midLon, midLat), Array(maxLon, maxLat))  // Search in top right rectangle
    // Bottom left
    val topright=quadrantTR.groupBy("tid","dateColumn","oid").count().filter("count>1")


    val quadrantBL = finalPoints
      .range(Array("lon", "lat"), Array(minLon, minLat), Array(midLon, midLat))  // Search in bottom left rectangle
    // Bottom right
    val bottomleft=quadrantBL.groupBy("tid","dateColumn","oid").count().filter("count>1")


    val quadrantBR = finalPoints
      .range(Array("lon", "lat"), Array(midLon, minLat), Array(maxLon, midLat))
    val bottomright=quadrantBR.groupBy("tid","dateColumn","oid").count().filter("count>1")


    //combine the results by union
    val trajInsamequad=topleft.select("tid","dateColumn","oid").union(topright.select("tid","dateColumn","oid"))
      .union(bottomleft.select("tid","dateColumn","oid")).union(bottomright.select("tid","dateColumn","oid"))

    //count num of rows in df which has points starting and ending in same quadrant
    val inCounts=trajInsamequad.count()

    // get points crossing quadrant
    val notInCount=total-inCounts

    // Save results to file
    val pw = new PrintWriter(new File(args(2)))
    pw.write(s"In the same quadrant: $inCounts\n")
    pw.write(s"In different quadrants: $notInCount\n")
    pw.close()

    println("Done")

    simba.stop()
  }
}