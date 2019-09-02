import scala.util.Properties

object Main {
  def main(args: Array[String]): Unit = {

    val streamingContext = Factory.createContext()
    val stream = Factory.createStream(streamingContext)
      .map(record => record.value)

    val hdfsMaster = Properties.envOrElse("DIGEST_HADOOP_NAMENODE", "hdfs://namenode:8020")

    stream.foreachRDD(rdd => {
      if(!rdd.isEmpty)
        rdd.saveAsTextFile(hdfsMaster + "/user/spark/vehiclelocation/data_" + System.currentTimeMillis)
    })

    Factory.start(streamingContext)
  }
}
