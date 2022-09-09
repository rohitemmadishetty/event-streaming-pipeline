package com.roe.es.app;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> inputData = new ArrayList<>();
        inputData.add("To be, or not to be,--that is the question:--");
        inputData.add("Whether 'tis nobler in the mind to suffer");
        inputData.add("The slings and arrows of outrageous fortune");
        inputData.add("Or to take arms against a sea of troubles,");


        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                startWordCount(env, inputData);

        // execute and print result
        counts.print();
    }

    public static DataSet<Tuple2<String, Integer>> startWordCount(ExecutionEnvironment env, List<String> lines) throws Exception {
        DataSet<String> text = env.fromCollection(lines);

        return text.flatMap(new LineSplitter()).groupBy(0).aggregate(Aggregations.SUM, 1);

    }
}
