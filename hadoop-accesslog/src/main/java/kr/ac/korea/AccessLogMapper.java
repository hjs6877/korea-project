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
     * Map의 출력 타입은 Text, Text로 정의한다.
     *
     * 1. AccessLogManager를 이용해서 접근 경로와 IP를 추출한다.
     * 2. path를 키로, IP를 값으로 해서 출력으로 내 보낸다.
     * 3. path가 같은 키는 Iterable에 추가.
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
