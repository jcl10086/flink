package org.jcl.hive

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.hive.HiveCatalog

object HiveSource {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)

    val name            = "myhive"
    val defaultDatabase = "ysk"
    val hiveConfDir     = "src/main/resources" // a local path

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("myhive", hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive")
//    tableEnv.sqlQuery("select count(*) from ysk_store_info")
//    val rs = tableEnv.sqlQuery("").execute()
//    rs.print()
    val data = tableEnv.executeSql("select count(*) from ysk_store_info")
    data.print()
  }
}
