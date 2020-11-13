package org.jcl

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("d://data.txt")
    val count = text.map(x => x.split(";")).map(x => (x(3),1)).keyBy(_._1)
//      .timeWindow(Time.seconds(5))
      .sum(1)
    count.print()
    env.execute("Window Stream WordCount")
  }
}
