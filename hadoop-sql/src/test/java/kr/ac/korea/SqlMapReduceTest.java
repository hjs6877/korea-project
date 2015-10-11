package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by ideapad on 2015-10-11.
 */
public class SqlMapReduceTest {
    MapDriver<LongWritable, Text, Text, LineItem> mapDriver1;
    ReduceDriver<Text, LineItem, Text, Text> reduceDriver1;
    MapReduceDriver<LongWritable, Text, Text, LineItem, Text, Text> mapReduceDriver1;

    @Before
    public void setUp(){
        SqlMapper1 mapper = new SqlMapper1();
        SqlReducer1 reducer = new SqlReducer1();
        mapDriver1 = MapDriver.newMapDriver(mapper);
        reduceDriver1 = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver1 = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    }

    @Test
    public void firstMapTest() throws IOException {
        mapDriver1.withInput(new LongWritable(),
                new Text("1|15519|785|1|17|24386.67|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|"));

        LineItem item = new LineItem();
        item.set("1", "1", 17, 0.04, 0.02);
        mapDriver1.withOutput(new Text("1"), item);
        mapDriver1.runTest();
    }

    @Test
    public void firstReduceTest() {
//        ArrayList<LineItem> values = new ArrayList<LineItem>();
        LineItem item1 = new LineItem();
        item1.set("1", "1", 17, 0.04, 0.02);

        LineItem item2 = new LineItem();
        item2.set("1", "2", 15, 0.02, 0.01);

//        values.add(new LineItem());

        reduceDriver1.withInput(new Text("1"), Arrays.asList(item1, item2));
        reduceDriver1.withOutput(new Text(""), new Text("1"));

        reduceDriver1.runTest();
    }

    @Test
    public void firstMapReduceTest() throws IOException {
        mapReduceDriver1.withInput(new LongWritable(),
                        new Text("1|15519|785|1|17|24386.67|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|"))
                .withInput(new LongWritable(),
                        new Text("1|6731|732|2|36|58958.28|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |"))
                .withInput(new LongWritable(),
                        new Text("3|430|181|1|45|59869.35|0.06|0.25|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|"))
                .withInput(new LongWritable(),
                        new Text("3|1904|658|2|49|88489.10|0.11|0.29|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|"));

        mapReduceDriver1.withOutput(new Text("row:1\t"), new Text("1\t\t\t1\t\t\t17.0\t0.04\t0.02"))
                        .withOutput(new Text("row:2\t"), new Text("1\t\t\t2\t\t\t36.0\t0.09\t0.06"))
                        .withOutput(new Text("row:3\t"), new Text("3\t\t\t1\t\t\t45.0\t0.06\t0.25"))
                        .withOutput(new Text("row:4\t"), new Text("3\t\t\t2\t\t\t49.0\t0.11\t0.29"));

        System.out.println("\t\tOrderkey\tLinenumber\tSUM\t\tMAX\t\tAVG");

        /**
         * orderkey 3이 linenumber 2로 두번째 group by가 되는 케이스. group by가 되면서 linenumber 2에 해당하는 값들에 대해서 sum, max, avg 값을 계산한다.
         */
//        mapReduceDriver1.withInput(new LongWritable(),
//                new Text("1|15519|785|1|17|24386.67|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|"))
//                .withInput(new LongWritable(),
//                        new Text("1|6731|732|2|36|58958.28|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |"))
//                .withInput(new LongWritable(),
//                        new Text("3|430|181|2|45|59869.35|0.06|0.25|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|"))
//                .withInput(new LongWritable(),
//                        new Text("3|1904|658|2|49|88489.10|0.11|0.29|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|"));
//
//        mapReduceDriver1.withOutput(new Text("row:1\t"), new Text("1\t\t\t1\t\t\t17.0\t0.04\t0.02"))
//                        .withOutput(new Text("row:2\t"), new Text("1\t\t\t2\t\t\t36.0\t0.09\t0.06"))
//                        .withOutput(new Text("row:3\t"), new Text("3\t\t\t2\t\t\t94.0\t0.11\t0.27"));


        mapReduceDriver1.runTest();
    }
}
