package posmining.okashige.SweetsCategory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;
import posmining.utils.graph.ArrangeTSVFile;

public class SweetsCategoryPrice {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SweetsCategoryPrice.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015017");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定f
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/okashige/sweets/sweetsCategoryPrice";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);

		//new ArrangeTSVFile().exportHeatmapData(outputpath, "part-r-00000", "table.csv");
	}


	/**
	 * Mapperクラスのmap関数を定義
	 * このメソッドは入力csvファイルの一行ごとに実行される．
	 * 最後にkeyとvalueをemitする．
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			//スイーツでないレシートは無視
			if(!PosUtils.isSweetsCode(csv[PosUtils.ITEM_CATEGORY_CODE])){
				return;
			}
			//valueとなる単価を取得
			String price = csv[PosUtils.ITEM_PRICE];

			//keyとなるスイーツの種類,店舗立地を取得
			String category = csv[PosUtils.ITEM_CATEGORY_NAME] +"\t"+ csv[PosUtils.LOCATION];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(category), new CSKV(price));
		}
	}


	/**
	 *  Reducerクラスのreduce関数を定義
	 *	このメソッドはkeyごとに一度実行される．
	 */
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {


			// 単価平均を求める
			int count = 0;
			int n = 0;
			for (CSKV value : values) {
				count += value.toInt();
				n++;
			}
			count = count/n;

			// emit
			context.write(key, new CSKV(count));
		}
	}
}
