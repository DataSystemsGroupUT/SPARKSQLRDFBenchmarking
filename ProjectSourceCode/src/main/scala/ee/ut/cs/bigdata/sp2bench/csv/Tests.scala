package ee.ut.cs.bigdata.sp2bench.csv

import java.io.{File, FileOutputStream}

object Tests {
  def main(args: Array[String]): Unit = {
    val fos = new FileOutputStream(new File(s"hdfs://172.17.77.48:9000/user/hadoop/RDFBench/logs_NP_250M/250M/csv/WPT/250M/250.txt"), true)

//    println("fos")
  }
}