package kr.ac.korea;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ideapad on 2015-09-29.
 */
public class IncomingDegreeDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, IOException {
        if (args.length != 2) {
            System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }

        /**
         * 해당 정점의 Incoming Degree를 계산하는 Job
         */
        Job job = new Job();
        job.setJarByClass(IncomingDegreeDriver.class);
        job.setJobName("Incoming Degree First Job");
        job.setMapperClass(IncomingDegreeMapper.class);
        job.setCombinerClass(IncomingDegreeReducer.class);
        job.setReducerClass(IncomingDegreeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * 첫번째 Job의 처리 결과를 저장하는 임시 경로.
         */
        Path tempPath = new Path("/temp");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tempPath);
        job.waitForCompletion(true);

        /**
         * 모든 정점의 Incoming Degree의 평균을 계산하는 Job
         */
        job = new Job();
        job.setJarByClass(IncomingDegreeDriver.class);
        job.setJobName("Incoming Degree Second Job");

        job.setMapperClass(IncomingDegreeMapper2.class);
        job.setCombinerClass(IncomingDegreeReducer2.class);
        job.setReducerClass(IncomingDegreeReducer2.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        /**
         * 첫번째 Job의 결과를 입력으로 받는다.
         */
        FileInputFormat.addInputPath(job, tempPath);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        System.exit(0);
    }
}
