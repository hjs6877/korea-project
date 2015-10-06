package kr.ac.korea;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-17.
 */
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * Map�� ��� Ÿ���� Text, Text�� �����Ѵ�.
     *
     * 1. AccessLogManager�� �̿��ؼ� ���� ��ο� IP�� �����Ѵ�.
     * 2. path�� Ű��, IP�� ������ �ؼ� ������� �� ������.
     * 3. path�� ���� Ű�� Iterable�� �߰�.
     */
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    private IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();


        String path = AccessLogManager.getAccessPath(value.toString());
        String ip = AccessLogManager.getAccessIp(val);

        outputKey.set(path);
        outputValue.set(ip);
        context.write(outputKey, outputValue);
    }
}
