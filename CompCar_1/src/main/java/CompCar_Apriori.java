import java.io.*;
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
import scala.Array;
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




public class CompCar_Apriori {


    public static void main(String[] args) throws Exception {
        double minSupport=0.002;
        int numPartition=10;
        int minCount=5;
        String output_path="C:\\Users\\Zeay\\IdeaProjects\\TestSpark\\output.txt";
        String logFile = "C:\\Users\\Zeay\\IdeaProjects\\TestSpark\\test_data2.csv"; // 换成你自己的路径
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

        //去除空项\
        JavaRDD<List<List<String>>> RDD4=RDD3.filter(x->!x.isEmpty());

        //展开，生成原始数据集
        JavaRDD<List<String>>transactions=RDD4.flatMap(x->x.iterator());
        List<List<String>> itemset0=transactions.collect();


        //Apriori算法
        //展开，获取候选1项集
        JavaRDD<String>RDD5=transactions.flatMap(x->x.iterator());

        //生成候选1项集
        JavaPairRDD<String,Integer>RDD6=RDD5.mapToPair(x->new Tuple2<>(x,1));
        JavaPairRDD<String,Integer> RDD7 = RDD6.reduceByKey((x, y) -> x + y);

        //按最小支持度计数过滤，生成频繁1项集
        JavaPairRDD<String,Integer> RDD8=RDD7.filter(x->x._2>=minCount);

        // 生成候选2项集列表
        JavaRDD<String>RDD9=RDD8.map(x->x._1);
        List<String>list1=RDD9.collect();
        List<List<String>>itemset2=new ArrayList<>();
        for(int i=0;i<list1.size()-1;i++){
            for(int j=i+1;j<list1.size();j++){
                List<String> l=new ArrayList<>();
                l.add(list1.get(i));
                l.add(list1.get(j));
                itemset2.add(l);
            }


        }
        //生成频繁2项集
        JavaRDD<List<String>>RDD10=sc.parallelize(itemset2);
        JavaPairRDD<List<String>,Integer>RDD_itemset2=RDD10.mapToPair(x->new Tuple2(x,1));
        JavaPairRDD<List<String>,Integer> RDD11 = RDD_itemset2.reduceByKey((x, y) -> x + y);
        JavaPairRDD<List<String>,Integer>RDD12=RDD11.filter(x->support_count(itemset0,x._1)>=minCount);
        //将频繁2项集写入频繁k文件
        writeFileContext(RDD12.collect(),output_path);
        //循环生成频繁k项集，终止条件为频繁k项集的元素个数小于k
        JavaPairRDD<List<String>,Integer> RDD_itemset=RDD12;



        int k=2;
        while(RDD_itemset.count()>k){
            //由频繁看项集生成频繁k+1项集
            k=k+1;
            JavaRDD<List<String>>RDD13=RDD11.map(x->x._1);
            List<List<String>>list2=RDD13.collect();
            List<List<String>>itemset=new ArrayList<>();
            //生成候选k+1项集列表
            for(int i=0;i<list2.size()-1;i++){
                for(int j=i+1;j<list2.size();j++){
                    //对于频繁k项集的任两个元素，求交集，若其长度等于k+1，则加入到候选k+1项集列表
                    List<String> l1=list2.get(i);
                    List<String> l2=list2.get(j);
                    l1.remove(l2);
                    l1.addAll(l2);
                    if(l1.size()==k)
                        itemset.add(l1);
                }
            }
            //生成频繁k项集
            JavaRDD<List<String>>RDD14=sc.parallelize(itemset);
            JavaPairRDD<List<String>,Integer>RDD15=RDD14.mapToPair(x->new Tuple2(x,1));
            JavaPairRDD<List<String>,Integer> RDD16 = RDD15.reduceByKey((x, y) -> x + y);
            RDD_itemset=RDD16.filter(x->support_count(itemset0,x._1)>=minCount);
            //将频繁k+1项集写入输出文件
            writeFileContext(RDD_itemset.collect(),output_path);

        }

        //for(Tuple2<List<String>, Integer> record: RDD11.take(100))
            //System.out.println(record+"\n");



        //测试，打印前100个频繁1项集以及频繁1项集大小
      // for( Tuple2<String,Integer> t:RDD8.take(100))
            //System.out.println(t);
      // System.out.println(RDD8.count()+"\n");


    }

    //计算支持度计数，即某一项出现在基项集中的次数
    public static int support_count(List<List<String>>base_set,List<String> ls){
        int count=0;
        for(List<String> l :base_set){
            List<String> temp=ls;
            temp.remove(l);
            //若基项集中的此项包含目标项，则目标项的支持度计数加1
            if(temp.isEmpty())
                count++;

        }
        return count;
    }

    //将频繁项集写入输出件，每一条的格式为 car1 car2 ... support_count，每条占一行
    public static void writeFileContext(List<Tuple2<List<String>, Integer> > list, String path) throws Exception {
        File file = new File(path);
        //如果没有文件就创建
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for(Tuple2<List<String>, Integer> tp:list){
            for(String s:tp._1)
                writer.write(s + " ");
            writer.write(tp._2+"\n");
        }


        writer.close();
    }

}
