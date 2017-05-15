package org.alitouka.spark.dbscan.mineTests

import org.alitouka.spark.dbscan.exploratoryAnalysis.DistanceToNearestNeighborDriver.createNearestNeighborHistogram
import org.alitouka.spark.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper
import org.alitouka.spark.dbscan.exploratoryAnalysis.NumberOfPointsWithinDistanceDriver.createNumberOfPointsWithinDistanceHistogram
import org.alitouka.spark.dbscan.{Dbscan, DbscanSettings, RawDataSet}
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.debug.Clock
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.{SparkConf, SparkContext}


object TestForHRMap {


  def runDBscan(sc: SparkContext, data: RawDataSet, outputPathModel: String): Unit = {
    val maxDistance = 16.0E-4
    val clusteringSettings = new DbscanSettings().withEpsilon(maxDistance).withNumberOfPoints(6)
    val model = Dbscan.train (data, clusteringSettings)
    println(model.clusteredPoints.count())
    println(model.noisePoints.count())

    val clusters = model.allPoints.groupBy(p=>p.clusterId)


    var min = clusters.flatMap((a) => a._2).map(p=>p.measure).min()
    val res = clusters
      .map((cluster) => {
        var sum = 0.0
        var numElems = 0
        val i = cluster._2.iterator
        while(i.hasNext) {
          sum += i.next().measure
          numElems += 1
        }
        val min = cluster._2.map(p=> p.measure).min
        val average = (sum / numElems)
        var offsetAverage = average -  min
        if(offsetAverage< 0)
          offsetAverage = 0
       new Point(cluster._2.head.coordinates,0,0,0,0,cluster._2.head.clusterId,offsetAverage)
      })
      println(min)

    //min = 50//res.map(p=>p.measure).min()

    res.foreach(a=>
      {
        val array = a.coordinates.toArray
        //var measure = a.measure - min
        //if(measure <0)
        //  measure = 0
        println(array(0) + "," +array(1) + "," + a.measure)
      }
      )
    try {
      // This method can rise an Exception
      IOHelper.saveClusteringResult(model, outputPathModel)
    } catch {
      case e: Exception => println(e)
    }
  }


  def main(args: Array[String]): Unit = {
    def csvToTest = "GPSc.csv"
    def currentDir = System.getProperty("user.dir");
    def resources = currentDir + "\\resources"
    def isel_db_samples = resources + "\\isel_db_samples"

    def pathToCsv = isel_db_samples + "\\" + csvToTest

    def outputFolder = resources + "\\results"

    def outHist =outputFolder + "\\" + csvToTest + "HIST"
    def outModel = outputFolder + "\\" + csvToTest + "MODEL"
    def outNgh = outputFolder + "\\" + csvToTest + "NGH"

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val data = IOHelper.readDatasetCustom(sc,pathToCsv)

    calcHist(sc,data,outHist)
    calcNearestNeighbors(sc,data,outNgh)
    runDBscan(sc,data,outModel)
  }


  def calcHist(sc: SparkContext,  data: RawDataSet, outputPathHist:String): Unit ={
    println(data.count())
    val clock = new Clock()
    val settings = new DbscanSettings()
    val partitioningSettings = new PartitioningSettings()
    val histogram = createNearestNeighborHistogram(data, settings, partitioningSettings)

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

    IOHelper.saveTriples(sc.parallelize(triples), outputPathHist)
    //val maxDistance = 7.728286113347807E-4
    clock.logTimeSinceStart("Estimation of distance to the nearest neighbor")
  }

  def calcNearestNeighbors(sc: SparkContext, data: RawDataSet, outputPathNeighbors: String) : Unit= {
    val settings = new DbscanSettings()
      .withEpsilon(8E-4)
      .withDistanceMeasure(DbscanSettings.getDefaultDistanceMeasure)

    val partitioningSettings = new PartitioningSettings()

    val histogram = createNumberOfPointsWithinDistanceHistogram(data, settings, partitioningSettings)

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

    IOHelper.saveTriples(sc.parallelize(triples),outputPathNeighbors)
    val clock = new Clock()
    clock.logTimeSinceStart("Calculation of number of points within " + 8E-4)
  }

}
