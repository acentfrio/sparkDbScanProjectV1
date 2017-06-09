package org.alitouka.spark.dbscan

/**
  * Created by acentfrio on 07.06.2017.
  */
class ViewPoint (
                val centerCoords: PointCoordinates,
                val extremeCoords: PointCoordinates,
                val averageMeasure: Double,
                val normalizedByLog: Double,
                val normalizedByMinMax: Double,
                val normalizedByCustomScale:Double,
                val north:Double,
                val south:Double,
                val east:Double,
                val west:Double) extends  Serializable {

  private def separator = ","
  override def toString(): String =
  {
    val builder = StringBuilder.newBuilder
    builder
      .append( centerCoords.array(0))
      .append( separator)
      .append( centerCoords.array(1))
      .append( separator)
      .append( extremeCoords.array(0))
      .append( separator)
      .append( extremeCoords.array(1))
      .append( separator)
      .append( averageMeasure)
      .append( separator)
      .append( normalizedByLog)
      .append( separator)
      .append( normalizedByMinMax)
      .append( separator)
      .append( normalizedByCustomScale)
      .append( separator)
      .append( north)
      .append( separator)
      .append( south)
      .append( separator)
      .append( east)
      .append( separator)
      .append( west)
      .toString()
  }
  override def hashCode (): Int = {
    centerCoords.hashCode()

  }
}
