package org.alitouka.spark.dbscan.util.io

import org.apache.spark.SparkContext
import scala.collection.mutable.WrappedArray.ofDouble
import org.alitouka.spark.dbscan.{DbscanModel, RawDataSet, ClusterId, PointCoordinates}
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan.spatial.Point

/** Contains functions for reading and writing data
  *
  */
object IOHelper {


  def readDatasetCustom(sc: SparkContext, path: String): RawDataSet = {
    val rawData = sc.textFile (path)
    val header  = rawData.first()
    rawData.filter(row=> row != header).map (
      line => {
        val ar = line.split(";").map(row => removeQuotes(row))
        val measure = ar(5).toDouble
        val p = new Point(ar.slice(1,3).map(_.toDouble))
        p.measure = measure
        p
        //new Point (line.split(separator).map( _.toDouble ))
      }
    )
  }

  def removeQuotes(a: String):String = {
    a.slice(1,a.length()-1)
  }
  /** Reads a dataset from a CSV file. That file should contain double values separated by commas
    *
    * @param sc A SparkContext into which the data should be loaded
    * @param path A path to the CSV file
    * @return A [[org.alitouka.spark.dbscan.RawDataSet]] populated with points
    */
  def readDataset (sc: SparkContext, path: String): RawDataSet = {
    val rawData = sc.textFile (path)

    rawData.map (
      line => {
        new Point (line.split(separator).map( _.toDouble ))
      }
    )
  }

  /** Saves clustering result into a CSV file. The resulting file will contain the same data as the input file,
    * with a cluster ID appended to each record. The order of records is not guaranteed to be the same as in the
    * input file
    *
    * @param model A [[org.alitouka.spark.dbscan.DbscanModel]] obtained from Dbscan.train method
    * @param outputPath Path to a folder where results should be saved. The folder will contain multiple
    *                   partXXXX files
    */

  def saveClusteringResult (model: DbscanModel, outputPath: String) {

    model.allPoints.map ( pt => {

      pt.coordinates.mkString(separator) + separator + pt.measure+ separator + pt.clusterId
    } ).saveAsTextFile(outputPath)
  }



  def saveClusteringResultWithParameters(res: RDD[Point], outputPathResults: String): Unit =
  {
    res.map(pt=>
    {
      pt.coordinates.mkString(separator)+ separator + pt.measure
    }).saveAsTextFile(outputPathResults)

  }
  private [dbscan] def saveTriples (data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map ( x => x._1 + separator + x._2 + separator + x._3 ).saveAsTextFile(outputPath)
  }

  private def separator = ","

}
