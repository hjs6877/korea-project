package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-29.
 */
public class IncomingDegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text vId = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vertexes = value.toString().split(" ");

        /**
         * Incoming Degree ����� �Ǵ� ������ Ű��, 1�� ������ context�� writing�Ѵ�.
         */
        String vertex = vertexes[1];
        vId.set(vertex);
        context.write(vId, one);
    }
}