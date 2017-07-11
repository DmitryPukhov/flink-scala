package dmitry.examples.flink

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment;
/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
object Job {


  def main(args: Array[String]) {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "hdfs://localhost:9000/data"
    //val stream = env.readTextFile("data/data.txt")
    val format = new TextInputFormat(new Path(filePath))
    format.setFilesFilter(FilePathFilter.createDefaultFilter)
    val typeInfo = BasicTypeInfo.STRING_TYPE_INFO
    format.setCharsetName("UTF-8")
    val stream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)

    stream.print()
    env.execute()

  }
}
