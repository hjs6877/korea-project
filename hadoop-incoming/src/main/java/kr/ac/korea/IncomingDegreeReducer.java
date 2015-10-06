package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-29.
 */
public class IncomingDegreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        /**
         * 1씩 더해서 Incoming Degree 계산.
         */
        for(IntWritable val : values){
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}