package pv.aggregation

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

trait TestSparkSessionProvider extends FlatSpec with Matchers with BeforeAndAfterEach {
  var sparkSession: SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .config("spark.master", "local")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

