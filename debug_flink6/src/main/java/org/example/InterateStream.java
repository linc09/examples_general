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

/**
 * Flink interateStream document: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/#iterations
 * This example show how to use interateStream. 迭代流中，大于2的值继续迭代、一直到减到小于2为止，符合条件的tuple可向下游传播被消费。
 */
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

        DataStreamSource<Tuple2<String, Long>> streamSource = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        //SingleOutputStreamOperator<Tuple2<String, Long>> streamSource = env.generateSequence(4,7).map(index -> new Tuple2<>(Long.valueOf(index % 10).toString(), Long.valueOf(index % 10))).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 开启迭代
        IterativeStream<Tuple2<String, Long>> it = streamSource.iterate();
        // 迭代算法
        DataStream<Tuple2<String, Long>> minusOne = it.map(index -> new Tuple2<>(index.f0, index.f1-1)).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 符合什么条件继续迭代
        it.closeWith(minusOne.filter(index -> index.f1 > 2));
        // 所有迭代结束后的event向下继续输出，包括参与迭代和未参与迭代的event
        DataStream<Tuple2<String, Long>> lessThanZero = minusOne.filter(index -> index.f1 <= 2);
        lessThanZero.print();
        /**
         * 2> (3,2)
         * 1> (8,2)
         * 1> (0,-1)
         * 2> (4,2)
         * 1> (9,2)
         * 1> (1,0)
         * 2> (5,2)
         * 1> (2,1)
         * 2> (6,2)
         * 1> (3,2)
         * 2> (7,2)
         * 1> (4,2)
         * 2> (8,2)
         */

        env.execute("Data Generator Source Example");

    }
}
