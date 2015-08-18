import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import java.util.Properties
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest

import scala.collection.JavaConversions._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.{SparkFiles, SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate => JodaDate, Weeks}

/**
 * Created by cloud0fan on 15/6/17.
 */
object Main {
  private[this] val format = DateTimeFormat.forPattern("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("yitong")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    assert(args(0).endsWith("db.conf"))
    sc.addFile(args(0))

    val DBConf = new Properties()
    val is = new FileInputStream(SparkFiles.get("db.conf"))
    DBConf.load(is)
    is.close()

    val URL = DBConf.getProperty("url")
    val USR = DBConf.getProperty("user")
    val PWD = DBConf.getProperty("password")

    // Europe shipping line code
    val codes = Seq("AT", "BE", "BG", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "GB")
    val prop = new Properties()
    prop.put("user", USR)
    prop.put("password", PWD)

    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.caseSensitive", "false")
    // A hack for oracle number type, getScale will return -127 for number.
    JdbcDialects.registerDialect(OracleDialect)

    val bill = sqlContext.read.jdbc(URL, "bak_xib.mskeir_biz_bill", prop)
    val ctnr = sqlContext.read.jdbc(URL, "bak_xib.mskeir_biz_ctnr", prop)
    // todo: need to fetch latest price info
    val shippingPrice = Files.readAllLines(Paths.get("/tmp/price"), StandardCharsets.UTF_8)
      .map(_.split(",").map(_.trim)).map(r => r(0) -> r(1).toDouble)

    val e = ctnr.agg(max('gate_out_time.substr(0, 8))).first().getAs[String](0)
    val last = JodaDate.parse(e, format).minusWeeks(2).toString(format)
    val earliest = shippingPrice.head._1

    val cleaned = ctnr.join(bill, ctnr("billid") === bill("id"))
      .where(bill("location").isNotNull)
      .where(bill("location") === "SGH")
      .where(ctnr("status") === "6")
      .where(bill("delivery_code").isNotNull)
      .where(ctnr("gate_out_time").isNotNull)
      .select(ctnr("ctn_type"), bill("delivery_code"), ctnr("gate_out_time").substr(0, 8).as("out_time"))
      .where('out_time > earliest && 'out_time < last)
      .filter(callUDF((s: String) => codes.exists(s.startsWith(_)), BooleanType, 'delivery_code))
      .filter('ctn_type === "40HIGH")
      .groupBy('out_time)
      .count()
      .withColumn("price", callUDF((s: String) => {
        shippingPrice.collectFirst {
          case (date, price) if s > earliest && date >= s => price
        }.get
      }, DoubleType, 'out_time))
      .select('out_time, 'price, 'count)
      .map(row => LabeledPoint(row.getLong(2), getFeatures(row.getString(0), row.getDouble(1))))
    cleaned.coalesce(1).saveAsTextFile("/tmp/result")

    val trainingData = cleaned

    val year = trainingData.map(_.features(0)).distinct().count().toInt
    val categoricalFeaturesInfo = Map[Int, Int](0 -> year, 1 -> 12, 2 -> 5, 3 -> 7, 4 -> 4)
    val numTrees = 1000
    val featureSubsetStrategy = "auto"
    val impurity = "variance"
    val maxDepth = 8
    val maxBins = 32

    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val startPredicateDate = JodaDate.parse(shippingPrice.last._1, format)
    val price = shippingPrice.last._2
    val output = Files.newBufferedWriter(Paths.get("/tmp/predication"), StandardCharsets.UTF_8)
    (0 to 6).map(startPredicateDate.plusDays(_).toString(format)).foreach { time =>
      val features = getFeatures(time, price)
      val res = model.predict(features)
      output.write(time + ": " + res)
      output.newLine()
    }
    output.close()
  }

  def getFestival(dt: JodaDate) = {
    if (isGuoQing(dt)) {
      1
    } else if (isSpringFestival(dt)) {
      2
    } else if (isYuanDan(dt)) {
      3
    } else 0
  }

  def isGuoQing(dt: JodaDate) = {
    if (dt.getMonthOfYear == 10 && dt.getDayOfMonth < 8) true else false
  }

  def isSpringFestival(dt: JodaDate) = {
    val tmp = dt.plusDays(2)
    val nl = ChinaDate.calElement(tmp.getYear, tmp.getMonthOfYear, tmp.getDayOfMonth)
    if (nl(1) == 1 && nl(2) <= 10) {
      true
    } else false
  }

  def isYuanDan(dt: JodaDate) = {
    if (dt.getMonthOfYear == 1 && dt.getDayOfMonth <= 3) true else false
  }

  def getFeatures(time: String, price: Double) = {
    val dt = JodaDate.parse(time, format)
    val year = dt.getYear - 2013
    val month = dt.getMonthOfYear - 1
    val weekOfMonth = Weeks.weeksBetween(dt.withDayOfMonth(1), dt).getWeeks()
    val week = dt.getDayOfWeek - 1
    val festival = getFestival(dt)

    Vectors.dense(year, month, weekOfMonth, week, festival, price)
  }

}
