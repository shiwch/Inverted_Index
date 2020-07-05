import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.util.TreeMap;

public class InvertedIndex {
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		// 统计词频时，需要去掉标点符号等符号，此处定义表达式
		private String pattern = "[^a-zA-Z0-9-]";

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 使用FileSplit实例提取文件名（含路径）
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            // 从文件名中获取文本名称
            String pathName = fileSplit.getPath().getName();
            
            // 将每一行转化为一个String                        
            String line = value.toString();
            // 将标点符号等字符用空格替换，这样仅剩单词
            line = line.replaceAll(pattern, " ");
            // 将String划分为一个个的单词
            String[] words = line.split("\\s+");
            int i = 0;
            // 将每一个单词初始化为词频为1，如果word相同，会传递给Reducer做进一步的操作
            for (String word : words) {
                if (word.length() > 0) {
                    //注意这里将map输出的（key-value）设置为（单词-文本名）的格式
                    context.write(new Text(word), new Text(pathName));
                }
            }
		}
	}

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 使用ArrayList实例来复制，values中出现的所有文本名
            ArrayList<String> Filename_List = new ArrayList<String>();
            // 使用Map存储 Key-文件名，Value-单词在文件名中出现次数(频率)
            Map<String, Integer> word_count = new HashMap<String, Integer>();

            // values中有重复的文本名，因为一个单词可能多次出现在统一文本中
            for (Text value : values) {                
                Filename_List.add(value.toString());
            }
            // 使用Set实例去除ArrayList中重复的文本名，方便之后进行统计
            Set<String> uniqueSet = new HashSet<String>(Filename_List);

            // 循环访问Set，对每一个非重复的文本名，统计在ArrayList中出现次数并存入Map中
            for(String filename : uniqueSet) {
                word_count.put(filename, Collections.frequency(Filename_List, filename));
            }
            
            // 使用ArrayLisrt存放word_count的Map.Entry
            ArrayList<Map.Entry<String, Integer> > sort_map = new ArrayList<Map.Entry<String, Integer> >(word_count.entrySet());
            
            // 将存放word_count的Map.Entry的ArrayList按照Map的Key排序
            Collections.sort(sort_map, new CompareByMapKey());
            // 将存放word_count的Map.Entry的ArrayList按照Map的Value排序
            Collections.sort(sort_map, new CompareByMapValue());
            
            String reducevalue = "\n\t";
            //循环访问存放word_count的Map.Entry的ArrayList，获取其中的文件名以及单词出现次数，构建格式化字符串
            for(Map.Entry<String, Integer> temp : sort_map) {
                reducevalue = reducevalue + "(" +String.format("%7s", temp.getKey()) + String.format(":%2d", temp.getValue()) + ")  ";
            }

            // 最后输出汇总后的结果
            context.write(key, new Text(reducevalue));
        }
            
        //排序比较函数
        public class CompareByMapKey implements Comparator<Map.Entry<String, Integer> >{
            //按照Map的Key排序，Key存放的是文件名
            @Override
            public int compare(Map.Entry<String, Integer> p1, Map.Entry<String, Integer> p2) {
                return p1.getKey().compareTo(p2.getKey());
            }
        }
        //排序比较函数
        public class CompareByMapValue implements Comparator<Map.Entry<String, Integer> >{
            //按照Map的Value排序，Value存放的是单词在文件中出现次数
            @Override
            public int compare(Map.Entry<String, Integer> p1, Map.Entry<String, Integer> p2) {
                return p2.getValue().compareTo(p1.getValue());
            }
        }
            
    }
 


	public static void main(String[] args) throws Exception {
		// 以下部分为HadoopMapreduce主程序的写法，对照即可
		// 创建配置对象
		Configuration conf = new Configuration();
		// 创建Job对象
		Job job = Job.getInstance(conf, "InvertedIndex");
		// 设置运行Job的类
		job.setJarByClass(InvertedIndex.class);
		// 设置Mapper类
		job.setMapperClass(InvertedIndexMapper.class);
		// 设置Reducer类
		job.setReducerClass(InvertedIndexReducer.class);
		// 设置Map输出的Key value     
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 设置Reduce输出的Key value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//设置输入输出的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 提交job
		boolean b = job.waitForCompletion(true);

		if(!b) {
			System.out.println("InvertedIndex task fail!");
		}

	}
}




