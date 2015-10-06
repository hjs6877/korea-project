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
         * ù��° Job�� ����� Incoming Degree�� ���κ��� ó��.
         * tab�� �������� split������ 1��° index�� �迭 ���� �ش� ������ Incoming Degree ��.
         * Incoming Degree�� ����� ���ϱ� ���ؼ��� ��� ������ total Incoming Degree ���� �ʿ��ϹǷ�, NullWritable �� Ű��
         * Incoming Degree ���� Iterable�� ���Եǵ��� �Ѵ�.
         */
        String[] incomingDegrees = value.toString().split("\t");
        String incomingDegree = incomingDegrees[1];
        double iDegree = Double.parseDouble(incomingDegree);
        iDegreeWritable.set(iDegree);
        context.write(NullWritable.get(), iDegreeWritable);
    }
}
