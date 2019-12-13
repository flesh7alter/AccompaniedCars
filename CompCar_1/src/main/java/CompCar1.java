import java.util.*;

import com.google.inject.internal.util.$Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.mllib.*;
import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.String;


class Tuplecompatrtor implements Comparator<Tuple2<String, String>> {


    public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {

        return o1._1.compareTo(o2._1);
    }
}




public class CompCar1 {
    public static void main(String[] args) {
        String logFile = "C:\\Users\\Zeay\\IdeaProjects\\TestSpark\\31.csv"; // 换成你自己的路径
        SparkConf conf = new SparkConf().setAppName("Test Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //每条数据(car_id,cross_id,time)转化为(cross_id,(time,car_id))
        JavaPairRDD<String,Tuple2<String,String>> RDD1 = sc.textFile(logFile)
                .mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String,Tuple2<String,String>> call(String s) throws Exception {
                        s=s.replace(" ","");
                        String[] parts = s.split(",");
                        Tuple2<String,String> tp1=new Tuple2<String, String>(parts[2],parts[0]);
                        Tuple2<String,Tuple2<String,String>> tp2=new Tuple2<String,Tuple2<String,String>>(parts[1],tp1);

                        return tp2;
                    }
                }).cache();
        //除去重复记录
        RDD1=RDD1.distinct();
        //按cross_id分组，生成(cross_id,iterable<(time,car_id)>）
        JavaPairRDD<String,Iterable<Tuple2<String,String>>>RDD2=RDD1.groupByKey();
        //排序，对每个(cross_id,iterable<(time,car_id)>，其terable<(time,car_id)部分按照time升序
        RDD2.persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> sorted_RDD = RDD2.mapValues(new Function<Iterable<Tuple2<String, String>>, // 输入
                Iterable<Tuple2<String, String>// 输出
                        >>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Iterable<Tuple2<String, String>> v1) throws Exception {
                List<Tuple2<String, String>> newList = Lists.newArrayList(v1);
                Collections.sort(newList, new Tuplecompatrtor());
                return newList;
            }
        });

        //生成数据集，即每个路口的伴随车原始数据集，得到[ [car11,car12...],[car21,car22...],...]
        JavaRDD<List<List<String>>>RDD3=sorted_RDD
                .map(new Function<Tuple2<String, Iterable<Tuple2<String, String>>>, List<List<String>>>() {
                    @Override
                    public List<List<String>> call(Tuple2<String, Iterable<Tuple2<String, String>>> t) throws Exception {
                        List<String> l=new ArrayList<String>();
                        List<List<String>> l_list = new ArrayList<List<String>>();
                        List<Tuple2<String, String>> newList = Lists.newArrayList(t._2);
                        for(int i=0;i<newList.size()-1;i++){
                            if(Long.parseLong(newList.get(i+1)._1)-Long.parseLong(newList.get(i)._1)<60){
                                l.add(newList.get(i)._2);
                                l.add(newList.get(i+1)._2);
                            }
                            else{
                                //去除重复项
                                LinkedHashSet<String> hashSet = new LinkedHashSet<String>(l);
                                ArrayList<String> l_withoutDuplicates = new ArrayList<String>(hashSet);
                                if(!l_withoutDuplicates.isEmpty())
                                    l_list.add(l_withoutDuplicates);
                                l.clear();

                            }
                        }

                        return l_list;
                    }
                });

        //去除空项
        JavaRDD<List<List<String>>> RDD4=RDD3.filter(x->!x.isEmpty());

        //展开，生成原始数据集
        JavaRDD<List<String>>transactions=RDD4.flatMap(x->x.iterator());
        //测试，打印10个路口的
        //for(List<String> record: RDD5.take(10))
            //System.out.println(record+"\n");

        //FPGrowth-Tree算法
        //设置最小支持度
        long num=transactions.count();
        double minSupport=5.0/num;
        System.out.println("minSupport: "+minSupport);
        int numPartition=10;
       FPGrowth fpGrowth = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition);
        FPGrowthModel<String> model = fpGrowth.run(transactions);

       for(FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect())
       System.out.println("[" + itemset.javaItems() + "]," + itemset.freq());

    }
}
