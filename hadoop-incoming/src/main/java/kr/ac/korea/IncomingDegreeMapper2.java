package kr.ac.korea;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-29.
 */
public class IncomingDegreeMapper2 extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {
    DoubleWritable iDegreeWritable = new DoubleWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 첫번째 Job의 결과인 Incoming Degree를 라인별로 처리.
         * tab을 기준으로 split했을때 1번째 index의 배열 값이 해당 정점의 Incoming Degree 임.
         * Incoming Degree의 평균을 구하기 위해서는 모든 정점의 total Incoming Degree 값만 필요하므로, NullWritable 을 키로
         * Incoming Degree 값을 Iterable에 포함되도록 한다.
         */
        String[] incomingDegrees = value.toString().split("\t");
        String incomingDegree = incomingDegrees[1];
        double iDegree = Double.parseDouble(incomingDegree);
        iDegreeWritable.set(iDegree);
        context.write(NullWritable.get(), iDegreeWritable);
    }
}
