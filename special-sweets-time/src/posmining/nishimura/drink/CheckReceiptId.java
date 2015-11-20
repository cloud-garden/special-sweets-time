package posmining.nishimura.drink;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import posmining.utils.CSKV;
import posmining.utils.PosUtils;
/**
 * @author Nishimura
 */
public class CheckReceiptId {
	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(CheckReceiptId.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015028");                   // ★自分の学籍番号

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
		String outputpath = "out/nishimura/drink/CheckReceiptId";     // ★MRの出力先
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
	}

	/**
	 * Mapperクラスのmap関数を定義
	 * このメソッドは入力csvファイルの一行ごとに実行される．
	 *　最後にkeyとvalueをemitしてあげる．
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

		//1つ前のレシートIDを比較するための変数
		public String receiptId_temp = "out";

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// csvファイルを配列に格納する
			String csv[] = value.toString().split(",");
			//スイーツ以外は無視
			if(!PosUtils.isSweetsCode(csv[PosUtils.ITEM_CATEGORY_CODE])){
				return;
			}
			//1つ前に読み取ったレシートIDと同じであれば無視(重複回避)
			else if(receiptId_temp.equals(csv[PosUtils.RECEIPT_ID])){
				return;
			}
			//比較用のレシートIDを保持
			receiptId_temp = csv[PosUtils.RECEIPT_ID];
			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(receiptId_temp), new CSKV(receiptId_temp));
		}
	}
	/**
	 *  Reducerクラスのreduce関数を定義
	 *	このメソッドはkeyごとに一度実行される．
	 */
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			// そのままにする
			context.write(key, null);
		}
	}
}