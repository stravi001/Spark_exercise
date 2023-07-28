object utils extends App {

  def getCount(inpColumn: String, inpWord: String): Int = {

    val outCount = (inpColumn.length - inpColumn.replace(inpWord, "").length) / inpWord.length

    outCount

  }

}
