package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class InterateStream {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        GeneratorFunction<Long, Tuple2<String, Long>> generatorFunction = index -> new Tuple2<>(Long.valueOf(index % 10).toString(), index % 10);

        DataGeneratorSource<Tuple2<String, Long>> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(4),
                        Types.TUPLE(Types.STRING, Types.LONG)
                );

        //DataStreamSource<Tuple2<String, Long>> streamSource = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        SingleOutputStreamOperator<Tuple2<String, Long>> streamSource = env.generateSequence(4,7).map(index -> new Tuple2<>(Long.valueOf(index % 10).toString(), Long.valueOf(index % 10))).returns(Types.TUPLE(Types.STRING, Types.LONG));


        IterativeStream<Tuple2<String, Long>> it = streamSource.iterate();

        DataStream<Tuple2<String, Long>> minusOne = it.map(index -> new Tuple2<>(index.f0, index.f1-1)).returns(Types.TUPLE(Types.STRING, Types.LONG));
        DataStream<Tuple2<String, Long>> stillGreaterThanZero = minusOne.filter(index -> index.f1 > 5);
        it.closeWith(stillGreaterThanZero);
        //stillGreaterThanZero.print();

        DataStream<Tuple2<String, Long>> lessThanZero = minusOne.filter(index -> index.f1 <= 5);
        lessThanZero.print();

        env.execute("Data Generator Source Example");

    }
}
