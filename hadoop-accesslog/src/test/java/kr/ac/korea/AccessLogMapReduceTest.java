package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
public class AccessLogMapReduceTest {
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp(){
        AccessLogMapper mapper = new AccessLogMapper();
        AccessLogReducer reducer = new AccessLogReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(),
                new Text("64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"));
        mapDriver.withOutput(new Text("/twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables"), new Text("64.242.88.10"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        ArrayList<Text> values = new ArrayList<Text>();
        values.add(new Text("64.242.88.10"));

        reduceDriver.withInput(new Text("/twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables"), values);
        reduceDriver.withOutput(new Text("Path: /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables"),
                new Text("\n" +
                        "COUNT: 1\nThe number of unique client IP: 1\n" +
                        "\n"));

        reduceDriver.runTest();
    }
}
