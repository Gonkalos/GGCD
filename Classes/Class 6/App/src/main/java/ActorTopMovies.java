import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ActorTopMovies {

    public static class MyComparator implements Comparator<Tuple2<String, Double>> {
        @Override
        public int compare(Tuple2<String,Double> value1, Tuple2<String,Double> value2) {
            if (value1._2 > value2._2) return -1;
            else return 1;
        }
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ActorMovieCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse name.basics file
        JavaPairRDD<String, String> name_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/name.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // filter actors
                .filter(l -> l[4].contains("actor") || l[4].contains("actress"))
                // create list of pairs (tconst, primaryName)
                .map(l -> {
                    String[] titles = l[5].split(",");
                    List<Tuple2<String, String>> pairs = new ArrayList<>();
                    for (String title : titles) {
                        pairs.add(new Tuple2<>(title, l[1]));
                    }
                    return pairs;
                })
                // iterate over list of pairs (tconst, primaryName)
                .flatMap(l -> l.iterator())
                // create pairs (tconst, primaryName)
                .mapToPair(l -> new Tuple2<>(l._1, l._2))
                // caching
                .cache();

        // parse title.basics file
        JavaPairRDD<String, String> title_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // filter movies
                .filter(l -> l[1].equals("movie"))
                // create pairs (tconst, primaryTitle)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                // caching
                .cache();

        // parse title.ratings file
        JavaPairRDD<String, Double> ratings = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.ratings.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])))
                // caching
                .cache();

        // merge the outputs of title_basics and ratings
        JavaPairRDD<String, Tuple2<String, Double>> values1 = title_basics.join(ratings); // (tconst, (title, rating))

        // merge the outputs
        List<Tuple2<String, Iterable<Tuple2<String, Double>>>> values2 = name_basics.join(values1) // (tconst, (actor, (title, rating)))
                .mapToPair(l -> new Tuple2<>(l._2._1, new Tuple2<>(l._2._2._1, l._2._2._2)))       // (actor, (title, rating))
                .groupByKey()                                                                      // (actor, [(title, rating), ...]
                .collect();

        // sort output
        for (Tuple2<String, Iterable<Tuple2<String, Double>>> value : values2) {
            List<Tuple2<String, Double>> list = new ArrayList<>(Lists.newArrayList(value._2));
            Collections.sort(list, new MyComparator());

            System.out.println(value._1 + ":");
            if (list.size() >= 3) {
                for (int i = 0; i < 3; i++) {
                    System.out.println("  > " + list.get(i)._1 + " (average rating = " + list.get(i)._2 + ")");
                }
            }
            else {
                for (int i = 0; i < list.size(); i++) {
                    System.out.println("  > " + list.get(i)._1 + " (average rating = " + list.get(i)._2 + ")");
                }
            }
        }
    }
}
