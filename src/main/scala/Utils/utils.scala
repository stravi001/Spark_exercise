package utils

class Utils {

  def defaultInputPath: String = "./src/main/resources/inputs"

  def defaultOutputPath: String = "./src/main/resources/outputs"

  def getCount(inpColumn: String, inpWord: String): Int = {

    val outCount = (inpColumn.length - inpColumn.replace(inpWord, "").length) / inpWord.length

    outCount

  }
}
