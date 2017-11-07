package com.saranyap.flink;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class SharedState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ControlData> control = env.fromElements(
                new ControlData(17),
                new ControlData(42));

        DataStream<Event> data = env.fromElements(
                new Event(2),
                new Event(42),
                new Event(6),
                new Event(17),
                new Event(8),
                new Event(42));

        DataStream<String   > result = control.broadcast()
                .connect(data)
                .flatMap(new MyConnectedStreams());

        result.print().setParallelism(1);

        env.execute();
    }

    static final class MyConnectedStreams implements CheckpointedFunction , CoFlatMapFunction<ControlData, Event, String> {

        private ListState<String> refDataState = null;

        @Override
        public void flatMap1(ControlData control, Collector<String> out) throws Exception {
            refDataState.add(control.toString());
        }

        @Override
        public void flatMap2(Event data, Collector<String> out) throws Exception {
            if(IteratorUtils.toList(refDataState.get().iterator()).contains(data.toString() ))
                out.collect(data.toString());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    // state name
                    "ref-data",
                    // type information of state
                    TypeInformation.of(new TypeHint<String>() {}));
            refDataState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        }
    }


    public static final class ControlData {

        public ControlData(int key) {
            this.key = key;
        }

        public int key;

        public String toString() {
            return String.valueOf(key);
        }
    }

    public static final class Event {

        public Event(int key) {
            this.key = key;
        }

        public int key;

        public String toString() {
            return String.valueOf(key);
        }
    }
}