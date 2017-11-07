package com.saranyap.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class App {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);


        Tuple3<Integer, String, String> emp1 = new Tuple3<>(1, "john", "smith");
        Tuple3<Integer, String, String> emp2 = new Tuple3<>(2, "Miley", "cyrus");
        Tuple3<Integer, String, String> emp3 = new Tuple3<>(3, "Mad", "Max");


        Tuple3<Integer,String, String> e1 = new Tuple3<>(1,"smith", "grade5");
        Tuple3<Integer,String, String> err = new Tuple3<>(4,"random", "some grade");
        Tuple3<Integer, String, String> e2 = new Tuple3<>(2,"cyrus", "grade3");
        Tuple3<Integer, String, String> e3 = new Tuple3<>(3,"Max1", "grade3");

        DataStreamSource<Tuple3<Integer, String, String>> stream1 = env.fromElements(emp1, emp2, emp3);
        DataStreamSource<Tuple3<Integer, String, String>> stream2 = env.fromElements(e1, e2, err, e3);
        DataStreamSource<Tuple2<Integer, Integer>> stream3 = env.fromElements(new Tuple2<>(1,100000),
                new Tuple2<>(2,50000),new Tuple2<>(3,60000));


        DataStream<Tuple5<Integer,String,String,String,Integer>> combinedStream = stream1
                .join(stream2)
                .where(t -> new Tuple2<>(t.f0,t.f2))
                .equalTo(t -> new Tuple2<>(t.f0, t.f1))
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply((first, second) -> new Tuple4<>(first.f0, first.f1, first.f2, second.f2))
                .join(stream3)
                .where(tuple -> tuple.f0)
                .equalTo((KeySelector<Tuple2<Integer, Integer>, Integer>) t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply((t4, newT2) -> new Tuple5(t4.f0,t4.f1,t4.f2,t4.f3, newT2.f1));



        SingleOutputStreamOperator<Tuple5<Integer, String, String, String, Integer>> reduce =
                combinedStream.filter(tuple -> tuple.f4 > 50000)
                .keyBy(3)//by grade
                .max(4);


       //Combined 3 Streams on multiple fields
//        combinedStream.print().setParallelism(1);

        //Groupby & Max function on Combined Streams
        reduce.print().setParallelism(1);

        env.execute();

    }
}
