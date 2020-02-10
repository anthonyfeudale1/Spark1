
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.lang.Integer.parseInt;

public class Spark1 {
    public static void main(String[] args) {
        String linkPath = args[0];
        String titlesPath = args[1];
        String query = args[2];
        String outputPath = args[3];
        JavaSparkContext sc = new JavaSparkContext("yarn", "Spark1",
                "$SPARK_HOME", new String[]{"target/Spark1-1.0.jar"});


        JavaPairRDD<Integer, ArrayList<Integer>> links = getFormattedLinks(linkPath, sc);
        JavaPairRDD<Integer, String> titles = getFormattedTitles(titlesPath, sc);
        JavaPairRDD<Integer, ArrayList<Integer>> isLinkedToo = getIsLinkedToo(links, sc);


        JavaPairRDD<Integer, String> rootSet = getRootSet(query, titles);
        if (rootSet.isEmpty()) {
            System.err.println("The search term '" + query + "' was not found in our pages.");
            return;
        }
        rootSet.saveAsTextFile(outputPath + "/RootSet");
        JavaRDD<Integer> baseSet = getBaseSet(rootSet, links);
        baseSet.saveAsTextFile(outputPath + "/BaseSet");
        computeAndSaveAuthAndHubScores(baseSet, links, isLinkedToo, titles, outputPath);
    }


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
        return temp.groupByKey().mapToPair(
                new PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, ArrayList<Integer>>() {
                    @Override
                    public Tuple2<Integer, ArrayList<Integer>> call(Tuple2<Integer, Iterable<Integer>> entry) {
                        ArrayList<Integer> temp = new ArrayList<Integer>();
                        for (int i : entry._2) {
                            temp.add(i);
                        }
                        return new Tuple2<Integer, ArrayList<Integer>>(entry._1, temp);
                    }
                });
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


    private static JavaPairRDD<Integer, String> getRootSet(String query, JavaPairRDD<Integer, String> titles) {

        Function<Tuple2<Integer, String>, Boolean> filter = k -> k._2.contains(query);
        return titles.filter(filter);


    }

    private static JavaRDD<Integer> getBaseSet(JavaPairRDD<Integer, String> rootSet, JavaPairRDD<Integer, ArrayList<Integer>> links) {
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

    private static void computeAndSaveAuthAndHubScores(JavaRDD<Integer> baseSet, JavaPairRDD<Integer, ArrayList<Integer>> links, JavaPairRDD<Integer, ArrayList<Integer>> isLinkedToo, JavaPairRDD<Integer, String> titles, String outputPath) {

        JavaPairRDD<Integer, Double> auths = baseSet.mapToPair(
                new PairFunction<Integer, Integer, Double>() {
                    public Tuple2<Integer, Double> call(Integer entry) {
                        return new Tuple2<Integer, Double>(entry, 1.0);
                    }
                }
        );
        JavaPairRDD<Integer, Double> hubs = auths;
        auths = filterAuths(auths, isLinkedToo);
        hubs = filterHubs(hubs, links);

        for (int i = 0; i < 25; ++i) {
            auths = iterateAuths(hubs);
            hubs = iterateHubs(auths);
        }
        JavaPairRDD<String, Double> sortedAuths = sortScores(auths.join(titles));
        JavaPairRDD<String, Double> sortedHubs = sortScores(hubs.join(titles));
        sortedAuths.saveAsTextFile(outputPath + "/AuthorityScores");
        sortedHubs.saveAsTextFile(outputPath + "/HubScores");


    }

    private static JavaPairRDD<Integer, Double> filterAuths(JavaPairRDD<Integer, Double> auths, JavaPairRDD<Integer, ArrayList<Integer>> isLinkedToo) {
        return auths.join(isLinkedToo.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Integer, ArrayList<Integer>>, Integer, Integer>() {
                    public Iterator<Tuple2<Integer, Integer>> call(Tuple2<Integer, ArrayList<Integer>> entry) {
                        ArrayList<Tuple2<Integer, Integer>> results = new ArrayList<Tuple2<Integer, Integer>>();
                        if (entry._2.size() == 0) {
                            results.add(new Tuple2<Integer, Integer>(entry._1, 0));
                        }
                        for (int i : entry._2()) {
                            results.add(new Tuple2<Integer, Integer>(i, entry._1));
                        }
                        return results.iterator();
                    }
                }
        )).mapToPair(
                new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Double>() {
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> entry) {
                        if (entry._2._2 == 0) {
                            return new Tuple2<Integer, Double>(entry._1, 0.0);
                        }
                        return new Tuple2<Integer, Double>(entry._2._2, entry._2._1);
                    }
                });
    }

    private static JavaPairRDD<Integer, Double> filterHubs(JavaPairRDD<Integer, Double> hubs, JavaPairRDD<Integer, ArrayList<Integer>> links) {
        return hubs.join(links.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Integer, ArrayList<Integer>>, Integer, Integer>() {
                    public Iterator<Tuple2<Integer, Integer>> call(Tuple2<Integer, ArrayList<Integer>> entry) {
                        ArrayList<Tuple2<Integer, Integer>> results = new ArrayList<Tuple2<Integer, Integer>>();
                        if (entry._2.size() == 0) {
                            results.add(new Tuple2<Integer, Integer>(entry._1, 0));
                        }
                        for (int i : entry._2()) {
                            results.add(new Tuple2<Integer, Integer>(i, entry._1));
                        }
                        return results.iterator();
                    }
                }
        )).mapToPair(
                new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Double>() {
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> entry) {
                        if (entry._2._2 == 0) {
                            return new Tuple2<Integer, Double>(entry._1, 0.0);
                        }
                        return new Tuple2<Integer, Double>(entry._2._2, entry._2._1);
                    }
                });
    }

    private static JavaPairRDD<String, Double> sortScores(JavaPairRDD<Integer, Tuple2<Double, String>> scores) {
        return scores.mapToPair(
                new PairFunction<Tuple2<Integer, Tuple2<Double, String>>, Double, String>() {
                    public Tuple2<Double, String> call(Tuple2<Integer, Tuple2<Double, String>> entry) {
                        return new Tuple2<Double, String>(entry._2._1, entry._2._2);
                    }
                }
        ).sortByKey(false).mapToPair(
                new PairFunction<Tuple2<Double, String>, String, Double>() {
                    public Tuple2<String, Double> call(Tuple2<Double, String> entry) {
                        return new Tuple2<String, Double>(entry._2, entry._1);
                    }
                });
    }

    private static JavaPairRDD<Integer, Double> iterateAuths(JavaPairRDD<Integer, Double> hubs) {

        JavaPairRDD<Integer, Double> scores = hubs.reduceByKey(
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                }
        );

        return normalize(scores);
    }

    private static JavaPairRDD<Integer, Double> iterateHubs(JavaPairRDD<Integer, Double> auths) {

        JavaPairRDD<Integer, Double> scores = auths.reduceByKey(
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                }
        );
        return normalize(scores);

    }

    private static JavaPairRDD<Integer, Double> normalize(JavaPairRDD<Integer, Double> scores) {
        Double sum = scores.map(
                new Function<Tuple2<Integer, Double>, Double>() {
                    public Double call(Tuple2<Integer, Double> entry) {
                        return entry._2;
                    }
                }
        ).reduce(
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) {
                        return aDouble + aDouble2;
                    }
                }
        );
        return scores.mapToPair(
                new PairFunction<Tuple2<Integer, Double>, Integer, Double>() {
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> entry) {
                        return new Tuple2<Integer, Double>(entry._1, entry._2 / sum);
                    }
                }
        );
    }
}