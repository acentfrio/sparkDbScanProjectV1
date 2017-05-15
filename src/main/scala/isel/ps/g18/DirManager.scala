package isel.ps.g18


class DirManager( csv: String) {


  def csvToTest = csv
  def currentDir = System.getProperty("user.dir")
  def resources = currentDir + "\\resources"
  def isel_db_samples = resources + "\\isel_db_samples"

  def pathToCsv = isel_db_samples + "\\" + csvToTest

  def outputFolder = resources + "\\results"

  def outHist =outputFolder + "\\" + csvToTest + "HIST"
  def outModel = outputFolder + "\\" + csvToTest + "MODEL"
  def outNgh = outputFolder + "\\" + csvToTest + "NGH"
  def outCSV = outputFolder + "\\" + csvToTest

}
