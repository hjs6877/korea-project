package kr.ac.korea;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-29.
 */
public class IncomingDegreeReducer2 extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double size = 0;

        double sum = 0;

        /**
         * NullWritable�� Ű�� ��� Incoming Degree ���� ���Ѵ�.
         */
        for(DoubleWritable val : values){
           // System.out.println("incoming degree:::: " + val);
            sum += val.get();
            size++;
        }

        /**
         * ��� ������ total Incoming Degree ��� ���.
         */
        double average = sum/size;
        System.out.println("Incoming Degree Average::::: " + average);
        result.set(average);
        context.write(key, result);
    }
}
