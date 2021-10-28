package ReadDataOnHive

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ReadData {
  def readHive(sparkSession: SparkSession, op: Commands): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
    import sparkSession.implicits._
    // from Hive
    // 传入日期dt参数
    val dt: String = op.dt

    // 自然人到公司边
    val naturalPerson2Corp: String =
      s"""
         |SELECT srcid
         |      ,dstid
         |      ,stock_ratio
         |FROM  ant_zmc.adm_zmep_graph_human2com_edge_df
         |WHERE dt=$dt
         |""".stripMargin
    // 公司到公司边
    val corp2Corp: String =
      s"""
         |SELECT srcid
         |      ,dstid
         |      ,stock_ratio
         |FROM  ant_zmc.adm_zmep_graph_com2com_edge_df
         |WHERE dt=$dt
         |""".stripMargin

    /**
     * 读取 readHive 中 Hive 表的每一行，进行处理。
     *
     * 比如在上面的代码中我们读取了两个表，每个表分别有三个字段 srcId, dstId, stock_ratio。
     * 首先将这三个字段转换为相应的数据结构（Long、String等等）。然后再进行后续操作。
     *
     * @param row 读取Hive表的每一行
     * @return
     */
    def extractRow(row: Row): Edge[Double] = {
      val srcId: VertexId = row.getAs[VertexId]("srcid")
      val dstId: VertexId = row.getAs[VertexId]("dstid")
      val stockRatio: String = row.getAs[String]("stock_ratio")

      val data: Double = if (stockRatio == null) 0.0 else stockRatio.toDouble  // 可能有为空的情况出现
      Edge(srcId, dstId, data)
    }

    val P2CData: DataFrame = sparkSession.sql(naturalPerson2Corp)
    // P2CData.show(100) // 展示前面100条

    // 读取自然人到公司
    val edgeNaturalPerson2Corp: Dataset[Edge[Double]] = P2CData.map((row: Row) => {
      extractRow(row)
    })
    // 读取公司到公司边表的数据
    val edgeCorp2Corp: Dataset[Edge[Double]] = sparkSession.sql(corp2Corp).map((row: Row) => {
      extractRow(row)
    })
    // 自然人到公司 + 公司到公司 合并形成边
    val unionEdgeRDD: RDD[Edge[Double]] = edgeNaturalPerson2Corp.union(edgeCorp2Corp).repartition(1000).rdd

    //构造图
    val defaultVertex = Map(9999999L -> Map(8888888L -> 0.00))
    Graph.fromEdges(unionEdgeRDD, defaultVertex)
  }
}
