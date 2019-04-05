package org.apache.spark.sql.simba.examples

/*
 * Part 2 task 2: Compute average number of visitors near Tiananmen Square per hour
 * */

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.sql.Timestamp

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.examples.Task1.POI
object Task5 {

  case class TrajectoryPoint(tid: Int, oid: Int, lon: Double, lat: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      println("Usage: " + this.getClass.getSimpleName + " NumThreads NumPartitions InputTraj InputPOI" +
        " Radius Output2008 Output2009")
      return
    }

    val simba = SimbaSession
      .builder()
      .master("local[" + args(0).toInt + "]")
      .appName("Project")
      .config("simba.index.partitions", args(1)) // Set the number of partitions via argument
      .getOrCreate()

    import simba.implicits._
    //read trajectories data
    val schema = ScalaReflection.schemaFor[TrajectoryPoint].dataType.asInstanceOf[StructType]
    val points = simba.read
      .option("header", "false")
      .schema(schema)
      .csv(args(2)) // Input CSV
      .as[TrajectoryPoint]


    println("Trajectory Points loaded")
    //read pois data
    val schemaPOI = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]
    val pois = simba.read
      .option("header", "false")
      .schema(schemaPOI)
      .csv(args(3)) // Input CSV
      .as[POI]


    println("POIs loaded")

    //filter data to select 2008 and only the week days
    val pointsTemp2008 = points.withColumn("Year",year(points("time")))
      .withColumn("month",month(points("time")))
      .withColumn("weekday",date_format(col("time"),"E"))
      .withColumnRenamed("lon","long").withColumnRenamed("lat","latt")
      .filter($"weekday" =!= "Sat" && $"weekday" =!= "Sun")
      .filter($"year"==="2008" )

    //filter data to select 2009 and only the week days
    val pointsTemp2009 = points.withColumn("Year",year(points("time")))
      .withColumn("month",month(points("time"))) //extract month from timestamp
      .withColumn("weekday",date_format(col("time"),"E"))
      .withColumnRenamed("lon","long").withColumnRenamed("lat","latt")
      .filter($"weekday" =!= "Sat" && $"weekday" =!= "Sun")
      .filter($"year"==="2009" )


    println("Trajectory points filtered")
    //select required attributes
    val trajpoints2008=pointsTemp2008.select("tid","oid","long","latt","year","month").distinct()
    val trajpoints2009=pointsTemp2009.select("tid","oid","long","latt","year","month").distinct()


    import simba.simbaImplicits._
    //build rtree index on trajectories dataframes
    trajpoints2008.index(RTreeType,"rtPoints",Array("long","latt"))
    trajpoints2009.index(RTreeType,"rtPoints09",Array("long","latt"))
    //build rtree index on POIs dataframe
    pois.index(RTreeType, "rtPois", Array("lon", "lat")) // R-Tree index over longitude and latitude

    println("Index done")

    //join POIs and trajectories using distance join
    val dataJoined2008=pois.distanceJoin(trajpoints2008,Array("lat","lon"),Array("latt","long"), args(4).toDouble)
      .distinct()
    val dataJoined2009=pois.distanceJoin(trajpoints2009,Array("lat","lon"),Array("latt","long"), args(4).toDouble)
      .distinct()

    //count points by grouping them
    val result2008=dataJoined2008.select("id","year","month","lon","lat")
      .groupBy("lon", "lat","year","month","id").count()
      .orderBy(desc("count")).limit(10) // sort in descending order to select top 10

    val result2009=dataJoined2009.select("id","year","month","lon","lat")
      .groupBy("lon", "lat","year","month","id").count().orderBy(desc("count")).limit(10)

    //select final results, id and its count
    val newres2009=result2009.select("id","count")
    val newres2008=result2008.select("id","count")
    // newres2008.coalesce(1).write.format("com.databricks.spark.csv").save("2008.csv")

    //write the result to csv file- separate files for each year
    val result1=newres2008.collect()
    val result2=newres2009.collect()

    val pw08 = new PrintWriter(new File(args(5)))
    for (row <- result1) {
      val id = row(0).asInstanceOf[Long]
      val count = row(1).asInstanceOf[Long]
      pw08.write(id + "," + count +"\n")
    }
    pw08.close()


    val pw09 = new PrintWriter(new File(args(6)))
    for (row <- result2) {
      val id = row(0).asInstanceOf[Long]
      val count = row(1).asInstanceOf[Long]
      pw09.write(id + "," + count +"\n")
    }
    pw09.close()

    println("Done")

    simba.stop()
  }
}