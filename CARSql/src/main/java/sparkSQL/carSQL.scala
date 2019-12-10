package sparkSQL

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{RowFactory, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/*
object carSQL {

  val master = "spark://10.141.212.225:7077"
  val appName = "carSQL"

  case class CarSchema(carId:Int, roadId:Int, time:Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.dynamicAllocation.enabled","false")

    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder()
      .appName(appName)
      //.enableHiveSupport()
      .getOrCreate()

    val lineRDD = sc.textFile("hdfs://10.141.212.225:9000/car/31.csv").cache()
    val rowsRDD = lineRDD.map(line => {
      val str = line.split(",")
      RowFactory.create(((str(0).toInt)),
        (str(1).toInt), (str(2).toLong))
    })
    rowsRDD

    val fields = collection.mutable.ListBuffer[StructField]()
    fields += DataTypes.createStructField("carId",DataTypes.LongType,true)
    fields += DataTypes.createStructField("roadId", DataTypes.LongType,true)
    fields += DataTypes.createStructField("timeStamp", DataTypes.LongType,true)

    val schema = DataTypes.createStructType(fields.toArray)
    schema.printTreeString()

    val dataSet = sparkSession.createDataFrame(rowsRDD, schema)
    dataSet.createTempView("car")

    val persons = sparkSession.sql("select * from car")
    val rows = persons.collect()

    for (s <- rows) {
      println(s)
    }

    //val result = textFile.flatMap(_.split(","))
    //  .map((_,1))
    //  .reduceByKey(_+_)
    //  .collect()
    //result.foreach(println)
  }
}
*/