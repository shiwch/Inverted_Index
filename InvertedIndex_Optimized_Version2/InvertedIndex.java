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
import java.util.Date;
import java.util.Calendar;
import java.util.Set;
import java.util.Comparator;
import java.util.TreeMap;
import java.nio.file.Files;
import java.nio.file.LinkOption;
//import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;

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
            // 定义index准备记录value的字符串的下标，初始化为0
            int index = 0;
            for (String word : words) {
                if (word.length() > 0) {
                    // value是文本中某一行，获取value字符串中word(value中的一个子字符串)后的所有字符
                    String remain_str = value.toString().substring(index, value.toString().length());
                    // 定义offset，计算当前的word在value字符串中的下标
                    int offset = index + (int)key.get() + remain_str.indexOf(word);
                    // 注意这里将map输出的（key-value）设置为（单词-文本名+$$$+偏移量）的格式
                    // $$$是用于分割文本名与偏移量的，后期方便取出文本名与偏移量
                    context.write(new Text(word), new Text(pathName + "$$$" + offset));
                    // 循环进行，我们需要将index更新为word后(word最后一个字符)的下标
                    index = index + remain_str.indexOf(word) + word.length();

                }
            }
        }
    }

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Reduce处理的是同一个单词的信息，某一个单词所处的文件名、偏移量将会送到一个
            // Reduce中处理，values(迭代器)就是从Map传递到Reduce的values，里面的数据结构
            // 是(文件名+$$$+偏移量)(Map函数中刚刚构建的字符串）
            
            // 使用ArrayList来存储values中出现的所有文本名
            ArrayList<String> Filename_List = new ArrayList<String>();
            // 使用Map，key为文件名，value为单词在该文件中的偏移量
            Map<String, ArrayList<Integer> > word_offset = new HashMap<String, ArrayList<Integer> >();
            // 使用WordMessage类型的ArrayList，用于存储单词所在各个文件的信息
            ArrayList<WordMessage> FILE = new ArrayList<WordMessage>();

			// values中有重复的文本名，因为一个单词可能多次出现在统一文本中
			for (Text value : values) {
                // 获取value的字符串
                String value_string = value.toString(); 
                // 从value中获取单词所在的文本名
                String filename = value_string.substring(0, value_string.indexOf("$$$")); 
                // 从value中获取单词在文本中的偏移量
                int offset = Integer.parseInt(value_string.substring(value_string.indexOf("$$$") + 3, value_string.length()));
                // 如果map内不存在filename的value，将filename-偏移量(arraylist存储)放入map中
                // 否则就向filename(key)的value中add一个偏移量
                if(word_offset.containsKey(filename)) {
                    word_offset.get(filename).add(offset);
                }
                else {
                    ArrayList<Integer> arrayList = new ArrayList<Integer>();
                    arrayList.add(offset);
                    word_offset.put(filename, arrayList);
                }
                Filename_List.add(filename); //向存储文本名的ArrayList中加入文本名
            }
			// 使用Set实例去除ArrayList中重复的文本名，方便之后进行统计
            Set<String> uniqueSet = new HashSet<String>(Filename_List);
            
            //为每一个含有Reduce说处理单词的文本，创建WordMessage类，存储单词在该文本内的全部信息(文本名、出现次数、偏移量等)
			for(String filename : uniqueSet) {
                // 创建WordMessage实例，使用文本名和单词出现次数初始化
                WordMessage wordMessage = new WordMessage(filename, Collections.frequency(Filename_List, filename));
                // 向wordMessage的内部变量offset中加入偏移量，单词在文本中出现多少次就有多少个偏移量
                for(int num : word_offset.get(filename)) {
                    wordMessage.add(num);
                }
                // 向wordMessage中加入文本创建时间
                wordMessage.setDate(getCreateTime("./input/" + filename));
                // 将每一个不重复的文本的wordMessage实例都存入FILE这个ArrayList内
                FILE.add(wordMessage);
			}

			// 对FILE这个ArrayList进行排序，使用的是先频数，再偏移量，再时间，再文本名首字母
            Collections.sort(FILE, new CompareByTime());
			
			// 一个单词reduce后的输出字符串
            String reducevalue = "\n";
            // 循环遍历这个单词所有的WordMessage实例(含有该单词的每一个文件的内关于该单词的具体信息)
			for(WordMessage WordMessage : FILE) {
                // 首先将WordMessage内的Offset列表进行排序，内部包含的是单词偏移量信息
                WordMessage.sort();  
                // 构建输出的reducevalue输出字符串
                String offset_string = "[";
                reducevalue = reducevalue + "   -----" + String.format("%7s", WordMessage.getFilename()) + String.format(":%2d", WordMessage.getWordCount()) + "  Offest";
                for(int i : WordMessage.getoffset()) {
                    offset_string = offset_string + i + " ";
                }
                offset_string = offset_string.trim();
                offset_string = offset_string + "]";
                reducevalue = reducevalue + String.format(" %-s", offset_string);
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                reducevalue = reducevalue + "Time " + dateFormat.format(WordMessage.getDate()) + "\n";
			}

			// 最后输出汇总后的结果
			context.write(key, new Text(reducevalue));
        }
        
        
        public class CompareByTime implements Comparator<WordMessage>{
			//按照姓名进行排序
			@Override
			public int compare(WordMessage p1, WordMessage p2) {
                int flag = 0;
                // 首先比较两个WordMessage实例中的单词出现次数
                flag = p2.getWordCount().compareTo(p1.getWordCount());
                if(flag == 0) {
                    // 其次比较两个WordMessage实例中单词在文本中第一次出现的位置也就是偏移量
                    flag = p1.getoffset().get(0).compareTo(p2.getoffset().get(0));
                }
                if(flag == 0) {
                    // 接下来比较两个WordMessage实例中文本的创建时间
                    flag = p2.getDate().compareTo(p1.getDate());

                }
                if(flag == 0) {
                    // 最后是比较两个WordMessage实例中的文本名
                    flag = p1.getFilename().compareTo(p2.getFilename());
                }
				return flag;
			}
        }
		// WordMessage类，用于存储单词在一个文件内的信息，出现次数等
        public class WordMessage {
            private String FileName; // 文件名
            // 单词在该文件内的偏移量
            private ArrayList<Integer> offset = new ArrayList<Integer>();
            private Integer WordCount;  //单词在该文件内出现的次数
            private Date date;          //该文件创建的日期

            // 初始化函数，使用文件名和单词出现次数
            public WordMessage(String FileName, Integer WordCount) {
                this.FileName = FileName;
                this.WordCount = WordCount;
            }
            // 设置date数据，文件创建时间
            public void setDate(Date date) {
                this.date = date;
            }
            // 获取单词所在的文件名
            public String getFilename() {
                return this.FileName;
            }
            // 获取单词在文件中出现次数
            public Integer getWordCount() {
                return this.WordCount;
            }
            // 获取单词在文件内出现的位置，相对于文本的偏移量
            public ArrayList<Integer> getoffset() {
                return this.offset;
            }
            // 获取文件创建时间
            public Date getDate() {
                return this.date;
            }
            // 向存储单词在文本内的偏移量的ArrayList中插入数组
            public void add(int num) {
                this.offset.add(num);
            }
            // 对偏移量的ArrayList进行排序
            public void sort() {
                Collections.sort(this.offset, (a, b) -> a.compareTo(b));
            }
        }
        // 获取文件的创建时间
        private static Date getCreateTime(String fullFileName){
            java.nio.file.Path path = java.nio.file.Paths.get(fullFileName);  
            BasicFileAttributeView basicview=Files.getFileAttributeView(path, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS );
            BasicFileAttributes attr;
            try {
                attr = basicview.readAttributes();
                Date createDate = new Date(attr.creationTime().toMillis());
                return createDate;
            } catch (Exception e) {
               e.printStackTrace();
            }
           Calendar cal = Calendar.getInstance();
           cal.set(1970, 0, 1, 0, 0, 0);
           return cal.getTime();
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





// public class CompareByFileName implements Comparator<WordMessage>{
//     //按照姓名进行排序
//     @Override
//     public int compare(WordMessage p1, WordMessage p2) {
//         return p1.getFilename().compareTo(p2.getFilename());
//     }
// }
// public class CompareByWordCount implements Comparator<WordMessage>{
//     //按照姓名进行排序
//     @Override
//     public int compare(WordMessage p1, WordMessage p2) {
//         int flag = 0;
//         flag = p2.getWordCount().compareTo(p1.getWordCount());
//         if(flag == 0) {
//             flag = p1.getFilename().compareTo(p2.getFilename());
//         }
//         return flag;
//     }
// }