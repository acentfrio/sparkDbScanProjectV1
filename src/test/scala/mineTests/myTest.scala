package org.alitouka.spark.dbscan.mineTests


import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.alitouka.spark.dbscan.{Dbscan, DbscanSettings}
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by acentfrio on 21.04.2017.
  */
object myTest {
  def main(args: Array[String]): Unit = {

    val path = "C:\\Users\\acentfrio\\Desktop\\alitoukaDBscan\\spark_dbscan\\src\\project\\9_400K.csv"
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val data = IOHelper.readDataset(sc, path)
    val clusteringSettings = new DbscanSettings ().withEpsilon(22).withNumberOfPoints(129)

    val model = Dbscan.train (data, clusteringSettings)

    try {
      // This method can rise an Exception
      IOHelper.saveClusteringResult(model, "C:\\Users\\acentfrio\\Desktop\\alitoukaDBscan\\spark_dbscan\\src\\project\\9_400Kresults")
    } catch {
      case e: Exception => println(e)
    }

    //val predictedClusterId = model.predict(new Point (100, 100))
    //println (predictedClusterId)
  }

}
