package com.knoldus.mnm

import com.knoldus.mnm.MnMCount.{loadMnMDataset, mnmCountOfCA}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class MnMCountTest extends FunSuite with BeforeAndAfterAll{
  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MnMCountTest")
      .master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading"){
    val sampleDf = loadMnMDataset(spark, "data/mnm_dataset.csv")
    val rCount = sampleDf.count()
    assert(rCount ==  99999,"record count should be 99999")
  }

  test("Aggregate Count of all colors"){
    val sampleDf = loadMnMDataset(spark, "data/mnm_dataset.csv")
    val aggregateColorCountForCA = mnmCountOfCA(sampleDf)
    val colorMap = new mutable.HashMap[String, Long]
    aggregateColorCountForCA.collect().foreach(r => colorMap.put(r.getString(1), r.getLong(2)))

    assert(colorMap("Yellow") == 1807, "Count for TX of yellow color mnm should be 1807")
    assert(colorMap("Green") == 1723, "Count for TX of green color mnm should be 1723")
    assert(colorMap("Brown") == 1718, "Count for TX of brown color mnm should be 1718")
    assert(colorMap("Orange") == 1657, "Count for TX of orange color mnm should be 1657")
    assert(colorMap("Red") == 1656, "Count for TX of red color mnm should be 1656")
    assert(colorMap("Blue") == 1603, "Count for TX of blue color mnm should be 1603")

  }

}
