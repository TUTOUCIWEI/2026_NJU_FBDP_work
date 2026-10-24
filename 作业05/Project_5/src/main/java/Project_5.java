import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Project_5 {
    
    public static class WordMap_A extends Mapper<LongWritable, Text, Text, Text> 
    {
        private Text outputKey = new Text();
        private Text label_A = new Text("A");

        @Override
        protected void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\s+");

            String indexId = fields[0];
            String wordStr = fields[1];

            outputKey.set(indexId + "\t" + wordStr);
            context.write(outputKey, label_A);
        }
    }

    public static class WordMap_B extends Mapper<LongWritable, Text, Text, Text> 
    {
        private Text outputKey = new Text();
        private Text label_B = new Text("B");

        @Override
        protected void map(
            LongWritable key, 
            Text value, 
            Context context
        ) throws IOException, InterruptedException 
        {
            String line = value.toString();
            String[] fields = line.split("\\s+");

            String indexId = fields[0];
            String wordStr = fields[1];

            outputKey.set(indexId + "\t" + wordStr);
            context.write(outputKey, label_B);
        }
    }

    public static class RelationReducer extends Reducer<Text, Text, Text, Text>
    {
        private final Text relationText = new Text();

        @Override
        protected void reduce(
            Text key, 
            Iterable<Text> values, 
            Context context
        ) throws IOException, InterruptedException 
        {
            boolean inA = false;
            boolean inB = false;
            for (Text val : values) {
                String source = val.toString();
                if (source.equals("A"))
                {
                    inA = true;
                }
                else if (source.equals("B"))
                {
                    inB = true;
                }
            }

            if (inA && inB) {
                relationText.set("BOTH");
            } else if (inA) {
                relationText.set("A_ONLY");
            } else if (inB) {
                relationText.set("B_ONLY");
            }

            context.write(key, relationText);
        }
    }

    public static class SummaryMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        private final Text indexKey = new Text();
        private final Text relationValue = new Text();

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split("\\s+");
            String indexId = fields[0];
            String relation = fields[2];

            indexKey.set(indexId);
            relationValue.set(relation);

            context.write(indexKey, relationValue);
        }
    }

    public static class SummaryReducer
        extends Reducer<Text, Text, Text, Text> {

    private final Text resultText = new Text();

    @Override
    protected void reduce(
            Text key,
            Iterable<Text> values,
            Context context
        ) throws IOException, InterruptedException {

            int bothCount = 0;
            int aOnlyCount = 0;
            int bOnlyCount = 0;

            for (Text value : values) {
                String relation = value.toString();

                if ("BOTH".equals(relation)) {
                    bothCount++;
                } else if ("A_ONLY".equals(relation)) {
                    aOnlyCount++;
                } else if ("B_ONLY".equals(relation)) {
                    bOnlyCount++;
                }
            }

            int unionCount =
                    bothCount + aOnlyCount + bOnlyCount;

            resultText.set(
                    bothCount + "\t"
                    + aOnlyCount + "\t"
                    + bOnlyCount + "\t"
                    + unionCount
            );

            context.write(key, resultText);
        }
    }

    public static void main(String[] args) throws Exception {


    if (args.length != 4) {
        System.err.println(
            "Usage: Project_5 "
            + "<A.txt> <B.txt> "
            + "<output_task1> <output_task2>"
        );

        System.exit(2);
    }        

        Configuration conf = new Configuration();
/* 
        //设置为本地模式运行
        conf.set("mapreduce.framework.name", "local");

        //使用本地Linus本地文件系统
        conf.set("fs.defaultFS", "file:///");
*/
        //创建Job实例
        Job job1 = Job.getInstance(conf, "Project_5 task 1");
        job1.setJarByClass(Project_5.class);

        //设置多个输入路径+设置输入文件Mapper类
        MultipleInputs.addInputPath(
            job1, 
            //new Path("/home/flower/FDBP/Project_5/input/A.txt"),
            new Path(args[0]),
            TextInputFormat.class, 
            WordMap_A.class);
        MultipleInputs.addInputPath(
            job1, 
            //new Path("/home/flower/FDBP/Project_5/input/B.txt"),
            new Path(args[1]),
            TextInputFormat.class, 
            WordMap_B.class);

        
        //设置输出格式
        job1.setOutputFormatClass(TextOutputFormat.class);

        //设置输入输出路径
        //FileInputFormat.addInputPath(job1, new Path("/home/flower/FDBP/Project_5/input/A.txt"));
        FileOutputFormat.setOutputPath(
            job1, 
            //new Path("/home/flower/FDBP/Project_5/output_task1")
            new Path(args[2]));

        //设置Mapper输出类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        //设置最终输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class); 

        //设置Mapper和Reducer类
        job1.setReducerClass(RelationReducer.class);
        job1.setNumReduceTasks(1);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(new Configuration(conf), "Project_5 task 2");

        job2.setJarByClass(Project_5.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(SummaryMapper.class);
        job2.setReducerClass(SummaryReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(
            job2,
            //new Path("/home/flower/FDBP/Project_5/output_task1")
            new Path(args[2])
        );

        FileOutputFormat.setOutputPath(
            job2,
            //new Path("/home/flower/FDBP/Project_5/output_task2")
            new Path(args[3])
        );

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}