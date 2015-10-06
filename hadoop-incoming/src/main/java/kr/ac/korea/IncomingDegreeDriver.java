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
         * �ش� ������ Incoming Degree�� ����ϴ� Job
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
         * ù��° Job�� ó�� ����� �����ϴ� �ӽ� ���.
         */
        Path tempPath = new Path("/temp");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tempPath);
        job.waitForCompletion(true);

        /**
         * ��� ������ Incoming Degree�� ����� ����ϴ� Job
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
         * ù��° Job�� ����� �Է����� �޴´�.
         */
        FileInputFormat.addInputPath(job, tempPath);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        System.exit(0);
    }
}
