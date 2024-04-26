package de.uni_leipzig.gradoop_datastream_api;

import org.apache.flink.api.common.functions.*;

import java.util.List;

/**
 * Represents a dataset in the context of the Flink DataStream API.
 * <p>
 * This interface provides methods to transform and manipulate data using the DataStream API,
 * imitating the DataSet API's functionality. Implementations of this interface are expected to be
 * immutable and thread-safe, designed to be used in a distributed data processing environment.
 */
public interface DataSet<T> {

    <R> DataSet<R> map(MapFunction<T, R> mapper);

    <R> DataSet<R> mapPartition(MapPartitionFunction<T, R> mapPartition) throws Exception;

    <R> DataSet<R> flatMap(FlatMapFunction<T, R> flatMapper) throws Exception;
    <R> DataSet<R> join(FlatMapFunction<T, R> flatMapper) throws Exception;

    DataSet<T> filter(FilterFunction<T> filter) throws Exception;

    DataSet<T> sum(int field);

    DataSet<T> max(int field);

    DataSet<T> min(int field);

    long count() throws Exception;

    List<T> collect() throws Exception;

    DataSet<T> reduce(ReduceFunction<T> reducer) throws Exception;

    <R> DataSet<R> reduceGroup(GroupReduceFunction<T, R> reducer) throws Exception;

    <R> DataSet<R> combineGroup(GroupCombineFunction<T, R> combiner) throws Exception;

}
