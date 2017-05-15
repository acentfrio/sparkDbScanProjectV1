package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.exploratoryAnalysis.DistanceToNearestNeighborDriver.createNearestNeighborHistogram
import org.alitouka.spark.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper
import org.alitouka.spark.dbscan.exploratoryAnalysis.NumberOfPointsWithinDistanceDriver.createNumberOfPointsWithinDistanceHistogram
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.debug.Clock
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{PairRDDFunctions, RDD}


object DBScanManager {



  object MyFunctions {
    def func1(cluster:ClusterId,it:Iterable[Point]): Point = {
      var sum = 0.0
      var numElems = 0
      val i = it.iterator
      while(i.hasNext) {
        sum += i.next().measure
        numElems += 1
      }
      val min = it.map(p=> p.measure).min
      val average = (sum / numElems)
      var offsetAverage = average -  min
      if(offsetAverage< 0)
        offsetAverage = 0
      return new Point(it.head.coordinates,0,0,0,0,it.head.clusterId,offsetAverage)
    }
  }

  @throws(classOf[Exception])
  def runDBscan(sc: SparkContext, data: RawDataSet,epsilon:Double, minPts:Int, outputPathModel: String, outputPathResults:String): Unit = {
    val clock = new Clock()
    val clusteringSettings = new DbscanSettings().withEpsilon(epsilon).withNumberOfPoints(minPts)
    val model = Dbscan.train (data, clusteringSettings)
    clock.logTimeSinceStart("DBScan clustering complete")

    clock.logTimeSinceStart("Points grouped by cluster")
    IOHelper.saveClusteringResult(model, outputPathModel)
    val clusters = model.allPoints.groupBy(p=>p.clusterId)


    var min = clusters.flatMap((a) => a._2).map(p=>p.measure).min()

    //val r = clusters.map(MyFunctions.func1)

    val res = clusters
      .map((cluster) => {

        val averageMeasure = cluster._2
          .map(p => p.measure)
          .sum /cluster._2.size


        val sumCords  = cluster._2
          .map(p=> p.coordinates)
          .reduce((c1,c2)=>{new PointCoordinates( Array( c1.array(0)+ c2.array(0),c1.array(1) + c2.array(1)))})
        val centerCoords = new PointCoordinates(Array(sumCords.array(0)/cluster._2.size,sumCords.array(1)/cluster._2.size))

        new Point(centerCoords,0,0,0,0,cluster._2.head.clusterId,averageMeasure)
      })

    IOHelper.saveClusteringResultWithParameters(res,outputPathResults)

  }



  def calcHist(sc: SparkContext,  data: RawDataSet, outputPathHist:String): Unit ={
    val clock = new Clock()
    val settings = new DbscanSettings()
    val partitioningSettings = new PartitioningSettings()
    val histogram = createNearestNeighborHistogram(data, settings, partitioningSettings)

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

    IOHelper.saveTriples(sc.parallelize(triples), outputPathHist)
    //val maxDistance = 7.728286113347807E-4
    clock.logTimeSinceStart("Estimation of distance to the nearest neighbor")
  }
  def calcNearestNeighbors(sc: SparkContext, data: RawDataSet, epsilon:Double ,outputPathNeighbors: String) : Unit= {
    val settings = new DbscanSettings()
      .withEpsilon(epsilon)
      .withDistanceMeasure(DbscanSettings.getDefaultDistanceMeasure)

    val partitioningSettings = new PartitioningSettings()

    val histogram = createNumberOfPointsWithinDistanceHistogram(data, settings, partitioningSettings)

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

    IOHelper.saveTriples(sc.parallelize(triples),outputPathNeighbors)
    val clock = new Clock()
    clock.logTimeSinceStart("Calculation of number of points within " + epsilon)
  }

}
