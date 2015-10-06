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
         * NullWritable을 키로 모든 Incoming Degree 값을 더한다.
         */
        for(DoubleWritable val : values){
           // System.out.println("incoming degree:::: " + val);
            sum += val.get();
            size++;
        }

        /**
         * 모든 정점의 total Incoming Degree 평균 계산.
         */
        double average = sum/size;
        System.out.println("Incoming Degree Average::::: " + average);
        result.set(average);
        context.write(key, result);
    }
}
