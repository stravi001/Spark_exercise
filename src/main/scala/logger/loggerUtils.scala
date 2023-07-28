import org.apache.log4j.PatternLayout
import org.apache.log4j.{Level, Logger, FileAppender}

object loggerUtils extends App {

  def vLogger(): Logger = {
    val fa = new FileAppender
    fa.setName("FileLogger")
    fa.setFile("logger.log")
    fa.setLayout(new PatternLayout("%m%n"))
    fa.setThreshold(Level.INFO)
    fa.setAppend(true)
    fa.activateOptions

    Logger.getRootLogger.addAppender(fa)

    Logger.getLogger("My Logger")

  }

}
