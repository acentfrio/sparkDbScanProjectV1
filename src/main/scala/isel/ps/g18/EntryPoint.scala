package isel.ps.g18

import org.alitouka.spark.dbscan.DBScanManager
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.{SparkConf, SparkContext}

object EntryPoint  extends  Serializable{

  def csvToTest = "GPSc.csv"
  def dirManager = new DirManager(csvToTest)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\bin")
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val data = IOHelper.readDatasetLatLon(sc,dirManager.pathToCsv)
    //val data = IOHelper.readDatasetTripleParameters(sc,dirManager.pathToCsv)

    //DBScanManager.calcHist(sc,data,dirManager.outHist) //extrair parametro epsilon
    val epsilon = 8E-4
    //DBScanManager.calcNearestNeighbors(sc,data,epsilon,dirManager.outNgh) // extrair minPts
    val minPts = 2
    DBScanManager.runDBscan(sc,data,epsilon,minPts, dirManager.outModel,dirManager.outCSV,dirManager.header)


  }
}
