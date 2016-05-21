import com.google.common.collect.Lists;
import com.xxo.mr.WordCountApp;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * mrunit Test
 * Created by xiaoxiaomo on 2016/5/20.
 */
public class WordCountAppTest {

    //单词统计Mapper
	private WordCountApp.WordCountMapper wordCountMapper;

    //单词统计Reducer
	private WordCountApp.WordCountReducer wordCountReducer;

    //Mapper和Reducer的Driver
	private MapDriver<LongWritable, Text, Text, LongWritable>  mapDriver;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	//private MapReduceDriver mrDriver;
	
	@Before
	public void before(){
		this.wordCountMapper = new WordCountApp.WordCountMapper();
		this.wordCountReducer = new WordCountApp.WordCountReducer();
		
		this.mapDriver = MapDriver.newMapDriver(wordCountMapper);
		this.reduceDriver = ReduceDriver.newReduceDriver(wordCountReducer);
        //也可以这样写：同时测试map和reduce
		//this.mrDriver = MapReduceDriver.newMapReduceDriver(wordCountMapper, wordCountReducer);
	}
	
	@Test
	public void testMap() throws IOException {
		//设置输入数据
		this.mapDriver.addInput(new LongWritable(0), new Text("blog\txiaoxiaomo"));
		this.mapDriver.addInput(new LongWritable(0), new Text("xxo\tblog"));
		this.mapDriver.addOutput(new Text("blog"), new LongWritable(1));
		this.mapDriver.addOutput(new Text("xiaoxiaomo"), new LongWritable(1));
		this.mapDriver.addOutput(new Text("xxo"), new LongWritable(1));
        this.mapDriver.addOutput(new Text("blog"), new LongWritable(1));

		this.mapDriver.runTest();
	}
	
	@Test
	public void testReduce() throws IOException{
		ArrayList<LongWritable> values = Lists.newArrayList(new LongWritable(1), new LongWritable(2));
		this.reduceDriver.addInput(new Text("xiaoxiaomo"), values);
		this.reduceDriver.addInput(new Text("blog"), values);

		this.reduceDriver.run();
	}
}
