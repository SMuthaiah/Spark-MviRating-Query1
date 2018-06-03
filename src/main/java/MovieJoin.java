
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MovieJoin {

    public static JavaSparkContext jsc;

    public MovieJoin(JavaSparkContext sc){
        this.jsc = sc;
    }

    /**
     * Function to modify the schema of the JavaPairRDD.
     * @param innerJoinRDD
     * @return
     */
    public static JavaPairRDD<String, Integer> modifyRDDSchema(JavaPairRDD<Integer, Tuple2<Double, String>> innerJoinRDD){

        JavaPairRDD<String, Integer> modifiedRDD = innerJoinRDD.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, String>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, Tuple2<Double, String>> integerTuple2Tuple2) throws Exception {
                return new Tuple2(integerTuple2Tuple2._2._2 + integerTuple2Tuple2._1, 1);
            }
        });

        return modifiedRDD;

    }

    /**
     * Function to Filter the first line from CSV file.
     * @param rddWhichNeedsToBeFiltered
     * @return
     */
    public static JavaRDD<String> filterHeaderFromRDD(JavaRDD<String> rddWhichNeedsToBeFiltered ){
        String header = rddWhichNeedsToBeFiltered.first();
        return rddWhichNeedsToBeFiltered.filter(row -> !row.equalsIgnoreCase(header));

    }

    /**
     *
     * This is the core logic of the job, where Transformation and action takes place.
     * @param movies
     * @param reviews
     * @return Sorted output
     */

    public static JavaPairRDD<Integer, String> run (String movies, String reviews){

        //Read the CSV files into RDD's
        JavaRDD<String> moviesRdd = jsc.textFile(movies);
        JavaRDD<String> reviewsRdd = jsc.textFile(reviews);

        //Remove the header from the RDD's before any transformation
        moviesRdd = filterHeaderFromRDD(moviesRdd);
        reviewsRdd = filterHeaderFromRDD(reviewsRdd);

        /*Split the lines based on comma operator and create a JavaPair rdd to store the movieid and movie name content
        as (1 ToyStory)*/
        JavaPairRDD<Integer, String> moviesPairRdd = moviesRdd.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String str) throws Exception {
                String[] data = str.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
                return new Tuple2<Integer, String>(Integer.parseInt(data[0]), data[1]);
            }
        });

        /*Split the lines based on comma operator and create a JavaPair rdd to store the movieid and review content
        as (31 2.5)*/
        JavaPairRDD<Integer, Double> reviewPairRdd = reviewsRdd.mapToPair(new PairFunction<String, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(String str) throws Exception {
                String[] data = str.split(",", -1);
                return new Tuple2<Integer, Double>(Integer.parseInt(data[1]), Double.parseDouble(data[2]));
            }
        });

        /*After joining the above two JavaPair RDD, We will have a schema like (Integer, Tuple<Double, String>), but
          we do not need the schema to be like for reducing it by key, hence we need to modify the schema to (String, Integer)
         */
        JavaPairRDD<String, Integer> modifiedRDD = modifyRDDSchema(reviewPairRdd.join(moviesPairRdd));

        //reduce transformation takes place here.
        JavaPairRDD<String, Integer> tempOutput = modifiedRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //Inverting the Key and Value
        JavaPairRDD<Integer, String> finalOutput = tempOutput.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2(tuple._2, tuple._1);
            }

        });

        JavaPairRDD<Integer, String> sortedOutput =  finalOutput.sortByKey(false);

        return sortedOutput;

    }


    /**
     * This is the entry point of the spark job, Spark context is initialized and closed here.
     * @param args
     * args[0], args[1] are the input csv files for the job
     * args[2] refers to the directory where the output will be stored.
     */

    public static void main(String args[]) {

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        MovieJoin mj = new MovieJoin(jsc);
        JavaPairRDD<Integer, String> output = mj.run(args[0],args[1]);
        output.saveAsHadoopFile(args[2], String.class, String.class, TextOutputFormat.class);
        jsc.close();

    }


}
