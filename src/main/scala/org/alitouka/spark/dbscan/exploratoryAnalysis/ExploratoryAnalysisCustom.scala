package org.alitouka.spark.dbscan.exploratoryAnalysis

import org.alitouka.spark.dbscan.{DbscanSettings, RawDataSet}
import org.alitouka.spark.dbscan.exploratoryAnalysis.DistanceToNearestNeighborDriver.createNearestNeighborHistogram
import org.alitouka.spark.dbscan.exploratoryAnalysis.NumberOfPointsWithinDistanceDriver.createNumberOfPointsWithinDistanceHistogram
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.debug.Clock
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.SparkContext


object ExploratoryAnalysisCustom {
  def calcHist(sc: SparkContext,  data: RawDataSet, outputPathHist:String): Unit ={
    val clock = new Clock()
    val settings = new DbscanSettings()
    val partitioningSettings = new PartitioningSettings()
    val histogram = createNearestNeighborHistogram(data, settings, partitioningSettings)
    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)
    IOHelper.saveTriples(sc.parallelize(triples), outputPathHist)
    clock.logTimeSinceStart("Estimation of distance to the nearest neighbor")
  }

  def calcNearestNeighbors(sc: SparkContext, data: RawDataSet, outputPathNeighbors: String, epsilon:Double) : Unit= {
    val settings = new DbscanSettings()
      .withEpsilon(epsilon)
      .withDistanceMeasure(DbscanSettings.getDefaultDistanceMeasure)
    val partitioningSettings = new PartitioningSettings()
    val histogram = createNumberOfPointsWithinDistanceHistogram(data, settings, partitioningSettings)
    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)
    IOHelper.saveTriples(sc.parallelize(triples),outputPathNeighbors)
    val clock = new Clock()
    clock.logTimeSinceStart("Calculation of number of points within " + 8E-4)
  }


}
