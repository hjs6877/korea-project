package kr.ac.korea;

import com.sun.tools.javac.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kjs on 2015-09-22.
 */
public class TestMapReduceTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp(){
        TestMapper mapper = new TestMapper();
        TestReducer reducer = new TestReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("655209;1;796764372490213;804422938115889;6"));
        mapDriver.withOutput(new Text("6"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        ArrayList<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("5"), values);
        reduceDriver.withOutput(new Text("5"), new IntWritable(1));
        reduceDriver.withOutput(new Text("5"), new IntWritable(1));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "655209;1;796764372490213;804422938115889;6")).withInput(new LongWritable(), new Text(
                "655209;1;796764372490213;804422938115889;6"));

        mapReduceDriver.withOutput(new Text("6"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("6"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

//    @Test
//    public void testMapper() throws IOException {
//        mapDriver.withInput(new LongWritable(), new Text("64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"));
//        mapDriver.withOutput(new Text("/twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables"), new Text("64.242.88.10"));
//        mapDriver.runTest();
//    }
}
