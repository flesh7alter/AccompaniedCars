package sparkSQL;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class carSQLdemo {

    public static class carInfo implements Serializable {

        private int carID;
        private int roadID;
        private Long timestamp;

        public int getCarID() {
            return carID;
        }

        public int getRoadID() {
            return roadID;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setCarID(int carID) {
            this.carID = carID;
        }

        public void setRoadID(int roadID) {
            this.roadID = roadID;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }

    public static void main(String [] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("spark://10.141.212.225:7077")
                .getOrCreate();

        JavaRDD<carInfo> carRDD = spark.read()
                .textFile("hdfs://10.141.212.225:9000/car/31.csv")
                .javaRDD()
                .map(new Function<String, carInfo>() {
                    @Override
                    public carInfo call(String lines) throws Exception {
                        String[] line = lines.split(",");
                        carInfo CARINFO = new carInfo();
                        CARINFO.setCarID(Integer.parseInt(line[0]));
                        CARINFO.setRoadID(Integer.parseInt(line[1]));
                        CARINFO.setTimestamp(Long.parseLong(line[2]));
                        return CARINFO;
                    }
                });

        Dataset<Row> carDF = spark.createDataFrame(carRDD, carInfo.class);
        carDF.createOrReplaceTempView("car");

        //test1
        Dataset<Row> carId1DF = spark.sql("SELECT * from car WHERE carID = 1");

        carId1DF.show();

        //TODO add your operation

        spark.stop();
    }

}

