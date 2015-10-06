package kr.ac.korea;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-17.
 */
public class AccessLogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(AccessLogDriver.class);
        job.setJobName("Access Log Count");
        job.setMapperClass(AccessLogMapper.class);
        //job.setCombinerClass(AccessLogReducer.class);         // Reduce�� 2�� ���Ƿ� ���ڿ��� �ι� ������.
        job.setReducerClass(AccessLogReducer.class);

        /**
         *  Map�� ��� key, value Ŭ���� ����.
         */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /**
         *  Reducer�� ��� key, value Ŭ���� ����.
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}
