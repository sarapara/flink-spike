package com.saranyap.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.List;

public class Broadcast {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Get a data set to be broadcasted
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("India", "USA", "UK").map(new RichMapFunction<String, String>() {
            private List<Integer> toBroadcast;
            // We have to use open method to get broadcast set from the context
            @Override
            public void open(Configuration parameters) throws Exception {
                // Get the broadcast set, available as collection
                this.toBroadcast =
                        getRuntimeContext().getBroadcastVariable("country");
            }

            @Override
            public String map(String input) throws Exception {
                int sum = 0;
                for (int a : toBroadcast) {
                    sum = a + sum;
                }
                return input.toUpperCase() + sum;
            }
        }).withBroadcastSet(toBroadcast, "country"); // Broadcast the set with name

        data.writeAsText("~/out.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }
}
