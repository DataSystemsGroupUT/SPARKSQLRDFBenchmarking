package ee.ut.cs.bigdata.sp2bench

import java.io.{File, FileOutputStream}

object RDFBenchMain {
  def main(args: Array[String]): Unit = {
    //    val fos = new FileOutputStream(new File(s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/logs_NP_250M/250M/csv/WPT/250M/250.txt"), true)
    //    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/250/csv/WPT/$ds$partitionType.txt"), true)

    val fos = new FileOutputStream(new File(s"/home/hadoop/RDFBenchMarking/logs/250M/csv/WPT/250.txt"), true)
    println(fos)
  }
}
