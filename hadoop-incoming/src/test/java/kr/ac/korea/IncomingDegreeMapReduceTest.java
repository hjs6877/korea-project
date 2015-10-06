package kr.ac.korea;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ideapad on 2015-10-03.
 */
public class IncomingDegreeMapReduceTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver1;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver1;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver1;

    MapDriver<LongWritable, Text, NullWritable, DoubleWritable> mapDriver2;
    ReduceDriver<NullWritable, DoubleWritable, NullWritable, DoubleWritable> reduceDriver2;
    MapReduceDriver<LongWritable, Text, NullWritable, DoubleWritable, NullWritable, DoubleWritable> mapReduceDriver2;

    @Before
    public void setUp(){
        IncomingDegreeMapper mapper = new IncomingDegreeMapper();
        IncomingDegreeReducer reducer = new IncomingDegreeReducer();
        mapDriver1 = MapDriver.newMapDriver(mapper);
        reduceDriver1 = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver1 = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        IncomingDegreeMapper2 mapper2 = new IncomingDegreeMapper2();
        IncomingDegreeReducer2 reducer2 = new IncomingDegreeReducer2();
        mapDriver2 = MapDriver.newMapDriver(mapper2);
        reduceDriver2 = ReduceDriver.newReduceDriver(reducer2);
        mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper2, reducer2);
    }

    @Test
    public void firstMapTest() throws IOException {
        mapDriver1.withInput(new LongWritable(), new Text("1 2"));
        mapDriver1.withOutput(new Text("2"), new IntWritable(1));
        mapDriver1.runTest();
    }

    @Test
    public void firstReduceTest() {
        ArrayList<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
//        values.add(new IntWritable(1));
        reduceDriver1.withInput(new Text("2"), values);
        reduceDriver1.withOutput(new Text("2"), new IntWritable(1));

        reduceDriver1.runTest();
    }

    /**
     * 첫번째 Job의 map, reduce Test
     *
     * @throws IOException
     */
    @Test
    public void firstMapReduceTest() throws IOException {
        mapReduceDriver1.withInput(new LongWritable(), new Text("1 2"))
                        .withInput(new LongWritable(), new Text("1 3"))
                        .withInput(new LongWritable(), new Text("2 3"));

        mapReduceDriver1.withOutput(new Text("2"), new IntWritable(1))
                        .withOutput(new Text("3"), new IntWritable(2));

        mapReduceDriver1.runTest();
    }


    @Test
    public void secondMapTest() throws IOException {
        mapDriver2.withInput(new LongWritable(), new Text("2\t1"));
        mapDriver2.withOutput(NullWritable.get(), new DoubleWritable(1.0));
        mapDriver2.runTest();
    }

    @Test
    public void secondReduceTest() throws IOException {
        ArrayList<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(1.0));
        reduceDriver2.withInput(NullWritable.get(), values);
        reduceDriver2.withOutput(NullWritable.get(), new DoubleWritable(1.0));
        reduceDriver2.runTest();
    }

    /**
     * 두번째 Job의 map, Reduce Test
     *
     * @throws IOException
     */
    @Test
    public void secondMapReduceTest() throws IOException {
        mapReduceDriver2.withInput(new LongWritable(), new Text("2\t1"))
                .withInput(new LongWritable(), new Text("3\t2"));

        mapReduceDriver2.withOutput(NullWritable.get(), new DoubleWritable(1.5));

        mapReduceDriver2.runTest();
    }
}
