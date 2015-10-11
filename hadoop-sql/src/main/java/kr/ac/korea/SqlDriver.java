package kr.ac.korea;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ideapad on 2015-10-11.
 */
public class SqlDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }

        /**
         * �ش� ������ Incoming Degree�� ����ϴ� Job
         */
        Job job = new Job();
        job.setJarByClass(SqlDriver.class);
        job.setJobName("Sql First Job");
        job.setMapperClass(SqlMapper1.class);
        job.setCombinerClass(SqlReducer1.class);
        job.setReducerClass(SqlReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LineItem.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * ù��° Job�� ó�� ����� �����ϴ� �ӽ� ���.
         */
        Path tempPath = new Path("/temp");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tempPath);
        job.waitForCompletion(true);
    }
}
