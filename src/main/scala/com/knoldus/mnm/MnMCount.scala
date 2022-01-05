package com.knoldus.mnm

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, count, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

object MnMCount extends Serializable {

  @transient val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    if(args.length < 1){
      logger.error("Usage: MnMCount filename")
      sys.exit(1)
    }

    logger.info("Starting M&M Count")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val mnmDF = loadMnMDataset(spark, args(0))
    mnmDF.show(5, truncate = false)
    val countMnMDF = aggregateCountOfAllColours(mnmDF)
    countMnMDF.show(60)
    logger.info(s"Total Rows == ${countMnMDF.count()}")

    logger.info("Preferred M&M in CA")
    val caMnMCountDF = mnmCountOfCA(mnmDF)
    caMnMCountDF.show()

    logger.info("Preferred M&M in TX")
    val txMnMCountDF = mnmCountOfTX(mnmDF)
    txMnMCountDF.show()

    logger.info("Finished M&M Count")
    spark.stop()

  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConfig = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("src/main/resources/spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConfig.set(k.toString, v.toString))
    sparkAppConfig
  }

  def loadMnMDataset(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataFile)
  }

  def aggregateCountOfAllColours(dfMnM: DataFrame): DataFrame = {
    dfMnM.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
  }

  def mnmCountOfCA(mnmDf: DataFrame): DataFrame = {
    mnmDf.select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
  }

  def mnmCountOfTX(mnmDf: DataFrame): DataFrame = {
    mnmDf.select("State", "Color", "Count")
      .where(col("State") === "TX")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
  }

}
