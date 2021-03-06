package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by kjs on 2015-10-02.
 */
public class TestReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws java.io.IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        System.out.println("sum: " + sum);
        context.write(key, new IntWritable(sum));
    }
}
