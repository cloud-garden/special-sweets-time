package posmining.nishimura.drink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import posmining.utils.graph.ArrangeTSVFile;

/**
 * @author Nishimura
 */
public class KindOfDrink {

	//一時的にスイーツを含むレシートIDを格納しておくためのリスト
	static List<String> receipt_id = new ArrayList<String>();

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(KindOfDrink.class);       // ★このファイルのメインクラスの名前
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
		String inputpath = "out/nishimura/have_sweets_ReceiptId.csv";
		String outputpath = "out/nishimura/table.csv";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		//レシートIDを読み込み
		read(receipt_id);

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

		int i=0;
		String item_category_code;
		String item_count;

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			if (i>= receipt_id.size()){
				String target = receipt_id.get(i);
				//同じレシートidでないデータは無視
				if(target !=csv[PosUtils.RECEIPT_ID]){
					return;
				}
				item_category_code = csv[PosUtils.ITEM_CATEGORY_CODE];
				item_count = csv[PosUtils.ITEM_COUNT];
			}

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(item_category_code), new CSKV(item_count));
		}
	}


	/**
	 *  Reducerクラスのreduce関数を定義
	 *	このメソッドはkeyごとに一度実行される．
	 */
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			Set<String> set = new HashSet<String>();
			for(CSKV ITEM_CATEGORY_CODE : values){
				set.add(ITEM_CATEGORY_CODE.toString());
			}

			// emit
			context.write(key, new CSKV(set.size()));
		}
	}
	private static void read(List<String> receipt_id) throws IOException{
		BufferedReader br = null;

		String file_path = "/out/nishimura/drink/";

		br = new BufferedReader(new FileReader(file_path));
		String line = null;
		// ファイルの読み込み
		while ((line = br.readLine()) != null) {
			String[] array = line.split(",");
			if (array.length == 2) {
				receipt_id.add(array[0]);
			}
		}
	}

}
