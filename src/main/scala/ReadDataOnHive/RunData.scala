package ReadDataOnHive

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object RunData {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    sparkSession.table("").select("", "")
    val options: Commands = Utils.bizDate(args, Map("degree" -> CommandItem(must = true, desciption = "度")))

    import sparkSession.implicits._
    val buildIntiGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = ReadData.readHive(sparkSession,
      op = options)
    val shareHoldingGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = CalculateStockRatio_V1
      .initStepAndTailFact(buildIntiGraph)
    val dataFrame: DataFrame = shareHoldingGraph.vertices.map((row: (VertexId, Map[VertexId, Map[VertexId, Double]])) => (row._1, row._2))
      .toDF("id", "values")

    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(s"temp_zmep_kg_stockCalculate_${options.dt}")
  }
}
