package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.exploratoryAnalysis.DistanceToNearestNeighborDriver.createNearestNeighborHistogram
import org.alitouka.spark.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper
import org.alitouka.spark.dbscan.exploratoryAnalysis.NumberOfPointsWithinDistanceDriver.createNumberOfPointsWithinDistanceHistogram
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.debug.Clock
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.SparkContext


object DBScanManager extends  Serializable{


  def fitToScale(averageMeasure: Double): Double =
  {
    if(averageMeasure< 20)
      return 2
    if(averageMeasure>=20 && averageMeasure<50)
      return 10
    if(averageMeasure>=50 && averageMeasure<60)
      return 20
    if(averageMeasure>=60 && averageMeasure<70)
      return 30
    if(averageMeasure>=70)
      return 40
    return 1
  }

  /*
      object MyFunctions {
        def func1(cluster:ClusterId,it:Iterable[Point]): Point = {

          return new Point(it.head.coordinates,0,0,0,0,it.head.clusterId,offsetAverage)
        }
      }
    */
  @throws(classOf[Exception])
  def runDBscan(sc: SparkContext, data: RawDataSet,epsilon:Double, minPts:Int, outputPathModel: String, outputPathResults:String, header:String): Unit = {
    //val clock = new Clock()
    val clusteringSettings = new DbscanSettings().withEpsilon(epsilon).withNumberOfPoints(minPts)
    val model = Dbscan.train (data, clusteringSettings)
    //clock.logTimeSinceStart("DBScan clustering complete")

    //clock.logTimeSinceStart("Points grouped by cluster")
    IOHelper.saveClusteringResult(model, outputPathModel)
    val clusters = model.allPoints.groupBy(p=>p.clusterId)

    val scaledMin = 0
    val scaledMax = 10
    var min = clusters.flatMap((a) => a._2).map(p=>p.measure).min()
    var max = clusters.flatMap((a) => a._2).map(p=>p.measure).max()
    //val r = clusters.map(MyFunctions.func1)
    println("Number of clusters:"+clusters.count())
    val res = clusters
      .map((cluster) =>
      {
       /* cluster._2.foreach(
          p=> println(p))*/

        //cluster average value
        val averageMeasure = cluster._2
          .map(p => p.measure)
          .sum /cluster._2.size


        //center point of cluster
        val sumCords  = cluster._2
          .map(p=> p.coordinates)
          .reduce((c1,c2)=>{new PointCoordinates( Array( c1.array(0)+ c2.array(0),c1.array(1) + c2.array(1)))})
        val centerCoords = new PointCoordinates(Array(sumCords.array(0)/cluster._2.size,sumCords.array(1)/cluster._2.size))

        //normalization
        val normalizedByLog=0
        //val normalizedByLog = 1+ log10(averageMeasure)
        val normalizedByMinMax = scaledMin + (scaledMax-scaledMin)/(max-min)*(averageMeasure-min)
        val normalizedByCustomScale = fitToScale(averageMeasure)

        // most distant point from center of the cluster
        var distanceFromCenter = 0.0
        var extremeCoords = new PointCoordinates(Array(0,0));
        cluster._2.foreach(p=>
        {
          val dst =
            Math.sqrt(
            Math.pow(p.coordinates.array(0)-centerCoords(0),2) +
            Math.pow(p.coordinates.array(1)-centerCoords(1),2))

          if(dst>distanceFromCenter)
            {
              distanceFromCenter = dst
              extremeCoords = p.coordinates
            }
        })
        var north = centerCoords(1)+distanceFromCenter
        var south = centerCoords(1)-distanceFromCenter
        var east = centerCoords(0)+distanceFromCenter
        var west = centerCoords(0)-distanceFromCenter

        new ViewPoint(centerCoords,extremeCoords,averageMeasure,normalizedByLog,normalizedByMinMax,normalizedByCustomScale,north,south,east,west);
      })

    IOHelper.saveClusteringResultMappedToViewPoint(res,outputPathModel,outputPathResults,header)
    //IOHelper.saveClusteringResultWithParameters(res,outputPathResults)

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
