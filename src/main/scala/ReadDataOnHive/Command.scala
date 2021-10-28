package ReadDataOnHive

import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.{HashMap => SHashMap}

case class Commands(dt: String, test: Boolean, version: String, addonMapOption: Map[String, String] = Map.empty)

case class TableField(name: String, typee: String, comment: String, isPartition: Boolean = false)

/**
 *
 * @param must 此参数是否是必须的。如果是必须的，这里就是must，否则不是必须的。
 * @param desciption 描述文字
 */
case class CommandItem(must: Boolean, desciption: String)

object Utils {
  /**
   * 从命令行中提取参数
   *
   * @param args  参数，比如业务日期 bizdate 等等
   * @param addOn 额外传入参数。这是一个Map，支持多个参数的传入。
   * @return
   */
  def bizDate(args: Array[String], addOn: Map[String, CommandItem] = Map.empty): Commands = {
    val options = new Options()
    val input = new Option("dt", true, "dt时间")
    input.setRequired(true)
    options.addOption(input)

    val inputTest = new Option("test", true, "测试")
    inputTest.setRequired(true)
    options.addOption(inputTest)

    val inputVersion = new Option("version", true, "版本")
    inputVersion.setRequired(true)
    options.addOption(inputVersion)

    val addonMapOption = new SHashMap[String, Option]()
    if (addOn.nonEmpty) {
      // 我总觉得这里的foreach可以改成map
      addOn.foreach((item: (String, CommandItem)) => {
        val name: String = item._1  // 名称
        val must: Boolean = item._2.must
        val desc: String = item._2.desciption
        val op = new Option(name, must, desc)
        options.addOption(op)
        addonMapOption.put(name, op)
      })
    }
    val parser = new BasicParser()
    val help = new HelpFormatter()
    var dt = "xxx"
    var test = false
    var version = "vv"
    val addonMap = new SHashMap[String, String]()
    try {
      val cmd = parser.parse(options, args);
      dt = cmd.getOptionValue("dt")
      test = cmd.getOptionValue("test").toBoolean
      version = cmd.getOptionValue("version")
      addonMapOption.foreach(row => {
        addonMap.put(row._1, cmd.getOptionValue(row._1))
      })
    } catch {
      case e: Exception => {
        print(e.getMessage())
        help.printHelp("必须要有dt", options)
        System.exit(1)
      }
    }

    val c = Commands(dt, test, version, addonMap.toMap)
    print(c)
    c

  }

  /**
   * 日期计算
   *
   * @param date 日期？
   * @param days 增加的天数？
   * @return
   */
  def dateBefore(date: String, days: Int): String = {
    val f = new SimpleDateFormat("yyyyMMdd");
    val today = f.parse(date);
    val c = Calendar.getInstance();
    c.setTime(today);
    c.add(Calendar.DAY_OF_MONTH, days); // 今天+1天
    val tomorrow = c.getTime();
    f.format(tomorrow)
  }

  /**
   * 昨天
   *
   * @param date
   * @return
   */
  def yesterday(date: String): String = {
    dateBefore(date, -1)
  }

  /**
   * 明天
   *
   * @param date
   * @return
   */
  def tommorow(date: String): String = {
    dateBefore(date, 1)
  }

  /**
   * unix time to yyyyMMdd
   *
   * @param date
   * @return
   */
  def unixTime(date: Long): String = {
    val today = new Date(date)
    val f = new SimpleDateFormat("yyyyMMdd");
    f.format(today)
  }


  def createTable(spark: SparkSession, tableName: String, comment: String, fields: List[TableField], lifecycle: Int): Unit = {
    spark.sql(createTableSQL(tableName, comment, fields, lifecycle))
  }

  /**
   *
   * @param tableName 表名称
   * @param comment 表说明
   * @param fields 表的所有的字段
   * @param lifecycle 生命周期
   * @return
   */
  def createTableSQL(tableName: String, comment: String, fields: List[TableField], lifecycle: Int): String = {


    val partition: List[TableField] = fields.filter(row => {
      row.isPartition
    })

    /*if (partition.length != 1) {
        throw new RuntimeException("目前支持建表需要有且需要一个分区字段")
    }*/
    val partitionFields: String = partition.map(row => {
      s"${row.name} ${row.typee} COMMENT '${row.comment}'"
    }).mkString(",\n")

    val common: List[TableField] = fields.filter(row => {
      !row.isPartition
    })

    if (common.length <= 0) {
      throw new RuntimeException("至少需要一个有效字段")
    }

    val commonFields = common.map(row => {
      s"${row.name} ${row.typee} comment '${row.comment}'"
    }).mkString(",\n")


    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS ${tableName}
         |(
         |   ${commonFields}
         |
         |)
         |PARTITIONED BY (
         |   ${partitionFields}
         |)
         |STORED AS ALIORC TBLPROPERTIES ( 'lifecycle' = '${lifecycle}' )
         |COMMENT '${comment}'
         |""".stripMargin

    sql
  }


  /**
   * 如果只有一个分区，请按建表的顺序进行赋值
   *
   * @param spark
   * @param df
   * @param targetTable
   * @param fields
   * @param partitionValue
   */
  def insertTable(spark: SparkSession, df: DataFrame, targetTable: String, fields: List[TableField], partitionValue: String): Unit = {

    val (temp: String, insertTable: String) = insertTableSQL(targetTable, fields, List(partitionValue))
    df.createOrReplaceTempView(temp)
    spark.sql(insertTable)
  }

  /**
   * 如果有多个分区，请按建表的顺序进行传值
   *
   * @param spark
   * @param df
   * @param targetTable
   * @param fields
   * @param partitionValues
   */
  def insertTable(spark: SparkSession, df: DataFrame, targetTable: String, fields: List[TableField], partitionValues: List[String]): Unit = {

    val (temp: String, insertTable: String) = insertTableSQL(targetTable, fields, partitionValues)
    df.createOrReplaceTempView(temp)
    spark.sql(insertTable)
  }

  def insertTableSQL(targetTable: String, fields: List[TableField], partitionValue: List[String]) = {
    val temp = s"temp_${targetTable}_${partitionValue.mkString("_")}"
    val partition = fields.filter(row => {
      row.isPartition
    })

    /*if (partition.length != 1) {
        throw new RuntimeException("目前支持建表需要有且需要一个分区字段")
    }*/
    val partitionFields = partition.zip(partitionValue).map(row => {
      s"${row._1.name}='${row._2}'"
    }).mkString(",\n")
    /*val partitionFields = partition.map(row => {
        s"${row.name}=${partitionValue}"
    }).mkString(",\n")*/

    val common = fields.filter(row => {
      !row.isPartition
    })

    if (common.length <= 0) {
      throw new RuntimeException("至少需要一个有效字段")
    }

    val commonFields = common.map(row => {
      s"${row.name}"
    }).mkString(",\n")
    val insertTable =
      s"""
         |INSERT OVERWRITE TABLE ${targetTable} PARTITION(${partitionFields})
         |SELECT
         |    ${commonFields}
         |FROM
         |    ${temp}
         |""".stripMargin
    println(insertTable)
    (temp, insertTable)
  }
}


