package org.alitouka.spark.dbscan.util.io

import java.io._
import javax.naming.spi.DirectoryManager

import isel.ps.g18.DirManager
import org.apache.spark.SparkContext
import org.alitouka.spark.dbscan._
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan.spatial.Point
import org.apache.commons.io.FileUtils

import scala.io.Source

/** Contains functions for reading and writing data
  *
  */
object IOHelper extends Serializable{

  def readDatasetLatLon(sc: SparkContext, path: String): RawDataSet = {
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
  def readDatasetTripleParameters(sc: SparkContext, path: String, param:String): RawDataSet = {
    val rawData = sc.textFile (path)
    val header  = rawData.first()
    val paramIdx = header.split(";").indexWhere(x=> x=="param")
      rawData.filter(row=> row != header).map (
      line => {
        val ar = line.split(";").map(row => removeQuotes(row))

        val measure = ar(paramIdx).toDouble
          val p = new Point((ar.slice(1,3) ++ ar.slice(5,6)).map(_.toDouble))
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
      pt.coordinates.array(0) + separator +   pt.coordinates.array(1) + separator + pt.measure
       // pt.coordinates.mkString(separator)+ separator + pt.measure
    }).saveAsTextFile(outputPathResults)

  }

  def saveClusteringResultMappedToViewPoint(res: RDD[ViewPoint], outputPathResults:String, outputPathCSV:String, header:String): Unit =
  {
    //sc.parallelize("lat,lon,averageSpeed,normalizedAverageSpeed").union(res).saveAsTextFile(outputPathResults);
    res.map(pt=> {
      pt.toString()
    }).saveAsTextFile(outputPathResults+"tmp")

    val file = Source.fromFile(
      new File(outputPathResults+"tmp")
        .listFiles()
        .filter(_.getName.startsWith("part"))(0)
        .getAbsolutePath)

    val outputFile = new File(outputPathCSV);
    val writer = new BufferedWriter(new FileWriter(outputFile));

    writer.write(header); // csv header
    writer.newLine();
    file.getLines().foreach(line=> //content
    {
      writer.write(line);
      writer.newLine();
    })
    writer.flush();
    writer.close();

    val csvName= outputPathCSV.split('.')(0)

    writeCsv(res.filter(p=> {p.averageMeasure<30}).collect().toList,header,csvName+"1")
    writeCsv(res.filter(p=> {p.averageMeasure>=30 && p.averageMeasure<40}).collect().toList,header,csvName+"2")
    writeCsv(res.filter(p=> {p.averageMeasure>=40 && p.averageMeasure<50}).collect().toList,header,csvName+"3")
    writeCsv(res.filter(p=> {p.averageMeasure>=50 && p.averageMeasure<60}).collect().toList,header,csvName+"4")
    writeCsv(res.filter(p=> {p.averageMeasure>=60 && p.averageMeasure<70}).collect().toList,header,csvName+"5")
    writeCsv(res.filter(p=> {p.averageMeasure>=70 && p.averageMeasure<80}).collect().toList,header,csvName+"6")
    writeCsv(res.filter(p=> {p.averageMeasure>=80 && p.averageMeasure<90}).collect().toList,header,csvName+"7")
    writeCsv(res.filter(p=> {p.averageMeasure>=90 }).collect().toList,header,csvName+"8")



    FileUtils.deleteQuietly(new File(outputPathResults+"tmp"));
  }

  def writeCsv(res: List[ViewPoint],header:String,fileName:String): Unit =
  {
    if(res.isEmpty)
      return

    val outputFile = new File(fileName+".csv");
    val writer = new BufferedWriter(new FileWriter(outputFile));

    writer.write(header); // csv header
    writer.newLine();
    res.foreach(p=>
    {
      writer.write(p.toString())
      writer.newLine()
    })
    writer.flush()
    writer.close()
  }
  private [dbscan] def saveTriples (data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map ( x => x._1 + separator + x._2 + separator + x._3 ).saveAsTextFile(outputPath)
  }

  private def separator = ","

}
