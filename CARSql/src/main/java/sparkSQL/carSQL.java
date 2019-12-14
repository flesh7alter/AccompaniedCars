import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.List;

public class carSQL {
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


    public static void writeFileContext(List<Row>result, String path) throws Exception {
        File file = new File(path);
        //如果没有文件就创建
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for (Row row:result){
            writer.write(row + "\r\n");
        }
        writer.close();
    }

    public static void main(String [] args) throws Exception {
        String file = args[0];
        String repeatedNum = args[1];
        System.out.println("Process file " + file);
        System.out.println("阈值" + repeatedNum);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("spark://10.141.212.225:7077")
                .getOrCreate();

        JavaRDD<carInfo> carRDD = spark.read()
                .textFile("hdfs://10.141.212.225:9000/"+file)
                .javaRDD()
                .map(new Function<String, carInfo>() {
                    @Override
                    public carInfo call(String lines) throws Exception {
                        String[] line = lines.split(",");
                        int lenTimestamp = line[2].length();
                        carInfo CARINFO = new carInfo();
                        CARINFO.setCarID(Integer.parseInt(line[0]));
                        CARINFO.setRoadID(Integer.parseInt(line[1]));
                        CARINFO.setTimestamp(Long.parseLong(line[2].substring(0,lenTimestamp-1)));
                        return CARINFO;
                    }
                });

        Dataset<Row> carDF = spark.createDataFrame(carRDD, carInfo.class);

        carDF.createOrReplaceTempView("car");

        Dataset<Row> carId1DF = spark.sql(
                "SELECT car1.carID as carID1, car2.carID as carID2  " +
                        "from car car1, car car2 " +
                        "WHERE car1.roadID = car2.roadID " +
                        "AND ABS(car1.timestamp-car2.timestamp) < 90 " +
                        "AND car1.carID != car2.carID");
        /**
         * +------+------+
         * | carID1| carID2|
         * +------+------+
         * |102605|102605|
         * |102605|103431|
         * |102605|103580|
         * |102605|103650|
         * |102605|103670|
         * |102605|105660|
         * |102605|105958|
         * |102605|106641|
         * |102605|107324|
         * |102605|112654|
         * |102605| 82823|
         * |102605|114474|
         * |102605| 90205|
         * |102605| 86752|
         * |102605|116160|
         * |102605| 28333|
         * |102605|117605|
         * |102605|118670|
         * |102605| 75061|
         * |102605|119640|
         */
        carId1DF.show();

        carId1DF.createOrReplaceTempView("accompany");


        //TODO 把伴随次数的界线设定为输入的参数
        Dataset<Row> carId2DF = spark.sql(
                "SELECT carID1, carID2 " +
                        "from accompany a " +
                        "WHERE (a.carID1, a.carID2) in " +
                        "(select carID1, carID2 from accompany " +
                        "group by carID1, carID2 " +
                        "having count(*) > "+repeatedNum+" )"
                        );

        /**
         * +------+------+
         * |carID1|carID2|
         * +------+------+
         * |     1| 23561|
         * |     1| 23561|
         * |     1| 25306|
         * |     1| 25306|
         * |     1| 25306|
         * |     3|  9596|
         * |     3|  9596|
         * |     3|  9596|
         * |     4| 29255|
         * |     4| 29255|
         * |     5| 15553|
         * |     5| 15553|
         * |     5| 18117|
         * |     5| 18117|
         * |     6| 15927|
         * |     6| 15927|
         * |     6| 15927|
         * |     6| 15927|
         * |     6| 24402|
         * |     6| 24402|
         * +------+------+
         */

        carId2DF.show();

        carId2DF.createOrReplaceTempView("accompany2");

        Dataset<Row> carId3DF = spark.sql(
                "SELECT carID1, carID2, COUNT(*) as repeatNum " +
                        "from accompany2  " +
                        "group by carID1, carID2 " +
                        "having count(*) > "+repeatedNum +" "+
                        "ORDER BY repeatNum DESC"
        );

        carId3DF.distinct();

        /***
         * carID1  carID2  repeatNum
         * [342658,125427,1280]
         * [125427,342658,1280]
         * [47522,101836,530]
         * [101836,47522,530]
         * [2605,56798,507]
         * [56798,2605,507]
         * [58925,248739,451]
         * [248739,58925,451]
         * [359714,346080,441]
         * [346080,359714,441]
         * [346080,362949,420]
         * [362949,359714,420]
         * [362949,346080,420]
         * [359714,362949,420]
         * [346080,362235,399]
         * [362235,346080,399]
         * [359714,362235,399]
         * [362235,359714,399]
         * [362949,362235,380]
         * [362235,362949,380]
         */
        carId3DF.show();

        List<Row> result =  carId3DF.collectAsList();

        writeFileContext(result, "result-all.txt");

        spark.stop();
    }
}
