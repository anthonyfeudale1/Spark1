
import io.netty.resolver.dns.DnsCache;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Array;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.lang.Integer.parseInt;
import static java.lang.Integer.rotateRight;

public class Spark1 {
    public static void main(String[] args) {
        String linkPath = args[0];
        String titlesPath = args[1];
        JavaSparkContext sc = new JavaSparkContext("yarn", "Spark1",
                "$SPARK_HOME", new String[]{"target/Spark1-1.0.jar"});


        JavaPairRDD<Integer, ArrayList<Integer>> links = getFormattedLinks(linkPath, sc).cache();
        JavaPairRDD<Integer, String> titles = getFormattedTitles(titlesPath, sc).cache();
        JavaPairRDD<Integer, ArrayList<Integer>> isLinkedToo = getIsLinkedToo(links, sc);

        String query = "a";
//        while (!query.equals("QUIT")) {
//            System.out.println("Please enter a search query: ");
//            Scanner in = new Scanner(System.in);
//            query = in.nextLine();

        JavaPairRDD<Integer, String> rootSet = getRootSet(query, titles, sc);
        rootSet.saveAsTextFile("/output/RootSet");
        JavaRDD<Integer> baseSet = getBaseSet(rootSet, links, sc);
        baseSet.saveAsTextFile("/output/BaseSet");
        computeAuthAndHubScores(baseSet, links, titles, sc);
    }

    //TODO: REWRITE THIS
    private static JavaPairRDD<Integer, ArrayList<Integer>> getIsLinkedToo(JavaPairRDD<Integer, ArrayList<Integer>> links, JavaSparkContext sc) {
        JavaPairRDD<Integer, Integer> temp = links.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Integer, ArrayList<Integer>>, Integer, Integer>() {
                    public Iterator<Tuple2<Integer, Integer>> call(Tuple2<Integer, ArrayList<Integer>> entry) {
                        ArrayList<Tuple2<Integer, Integer>> results = new ArrayList<Tuple2<Integer, Integer>>();
                        for (int i : entry._2()) {
                            results.add(new Tuple2<Integer, Integer>(i, entry._1));
                        }
                        return results.iterator();
                    }
                }
        );
        temp.groupByKey()
      return links;
    }


    private static JavaPairRDD<Integer, ArrayList<Integer>> getFormattedLinks(String linkPath, JavaSparkContext sc) {
        JavaRDD<String> links = sc.textFile(linkPath);
        return links.mapToPair(
                new PairFunction<String, Integer, ArrayList<Integer>>() {
                    public Tuple2<Integer, ArrayList<Integer>> call(String s) {
                        String[] temp = s.split(":");
                        int key = parseInt(temp[0]);
                        ArrayList<Integer> intLinks = new ArrayList<Integer>();
                        if (temp.length > 1) {
                            String[] stringLinks = temp[1].split(" ");
                            for (String stringLink : stringLinks) {
                                if (!stringLink.equals("")) {
                                    intLinks.add(parseInt(stringLink));
                                }
                            }
                        }
                        return new Tuple2<Integer, ArrayList<Integer>>(key, intLinks);
                    }
                }
        );

    }

    private static JavaPairRDD<Integer, String> getFormattedTitles(String titlesPath, JavaSparkContext sc) {
        JavaRDD<String> titles = sc.textFile(titlesPath);


        return titles.zipWithIndex().mapToPair(
                new PairFunction<Tuple2<String, Long>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Long> t) {
                        return new Tuple2<Integer, String>(Math.toIntExact(t._2) + 1, t._1);
                    }
                }
        );
    }


    private static JavaPairRDD<Integer, String> getRootSet(String query, JavaPairRDD<Integer, String> titles, JavaSparkContext sc) {

        Function<Tuple2<Integer, String>, Boolean> filter = k -> k._2.contains(query);
        return titles.filter(filter);


    }

    private static JavaRDD<Integer> getBaseSet(JavaPairRDD<Integer, String> rootSet, JavaPairRDD<Integer, ArrayList<Integer>> links, JavaSparkContext sc) {
        JavaRDD<Integer> rootSetNums = rootSet.map(
                new Function<Tuple2<Integer, String>, Integer>() {
                    public Integer call(Tuple2<Integer, String> entry) {
                        return entry._1;
                    }
                }
        );
        List<Integer> rootSetNumsCollected = rootSetNums.collect();
        JavaRDD<Integer> rootSetPointsTo = links.join(rootSet)
                .map(
                        new Function<Tuple2<Integer, Tuple2<ArrayList<Integer>, String>>, ArrayList<Integer>>() {
                            public ArrayList<Integer> call(Tuple2<Integer, Tuple2<ArrayList<Integer>, String>> entry) {
                                return new ArrayList<Integer>(entry._2._1);
                            }
                        }
                ).flatMap(
                        new FlatMapFunction<ArrayList<Integer>, Integer>() {
                            public Iterator<Integer> call(ArrayList<Integer> integers) {
                                return integers.iterator();
                            }
                        }
                );
        Function<Tuple2<Integer, ArrayList<Integer>>, Boolean> filter = k -> !Collections.disjoint(k._2, rootSetNumsCollected);
        JavaRDD<Integer> linksToRootSet = links.filter(filter)
                .map(
                        new Function<Tuple2<Integer, ArrayList<Integer>>, Integer>() {
                            public Integer call(Tuple2<Integer, ArrayList<Integer>> entry) {
                                return entry._1;
                            }
                        }
                );
        return rootSetNums.union(rootSetPointsTo).union(linksToRootSet).distinct();
    }

    private static void computeAuthAndHubScores(JavaRDD<Integer> baseSet, JavaPairRDD<Integer, ArrayList<Integer>> links, JavaPairRDD<Integer, String> titles, JavaSparkContext sc) {

        JavaPairRDD<Integer, Double> auths = baseSet.mapToPair(
                new PairFunction<Integer, Integer, Double>() {
                    public Tuple2<Integer, Double> call(Integer entry) {
                        return new Tuple2<Integer, Double>(entry, 1.0);
                    }
                }
        );
        JavaPairRDD<Integer, Double> hubs = auths;

        //auths = iterateAuths(auths);
    }

//    private static JavaPairRDD<Integer, Double> iterateAuths(JavaPairRDD<Integer, Double> auths) {
//    }


}


//        String inFile = args[0]; // Should be some file on your system
//        JavaSparkContext sc = new JavaSparkContext("yarn", "Spark1",
//                "$SPARK_HOME", new String[]{"target/Spark1-1.0.jar"});
//        JavaRDD<String> logData = sc.textFile(inFile).cache();
//
//        long numAs = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) {
//                return s.contains("a");
//            }
//        }).count();
//
//        long numBs = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) {
//                return s.contains("b");
//            }
//        }).count();
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//
//        logData.saveAsTextFile(args[1]);
//        sc.stop();