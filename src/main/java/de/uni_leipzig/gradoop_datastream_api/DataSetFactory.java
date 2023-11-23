package de.uni_leipzig.gradoop_datastream_api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Factory for creating instances of {@link DataSet}.
 */
public final class DataSetFactory {

    private DataSetFactory() {
        // Private constructor to prevent instantiation
    }

    /**
     * Creates an instance of {@link DataSet}.
     *
     * @param env the StreamExecutionEnvironment
     * @param internalStream the underlying DataStream
     * @param type the TypeInformation of the dataset
     * @param <T> the type of elements in the DataSet
     * @return a new instance of DataSet
     */
    public static <T> DataSet<T> createDataSet(StreamExecutionEnvironment env, DataStream<T> internalStream, TypeInformation<T> type) {
        return new DataSetImpl<>(env, internalStream, type); // Assume DataSetImpl is the concrete implementation
    }

    // Additional factory methods can be added here if needed
}
