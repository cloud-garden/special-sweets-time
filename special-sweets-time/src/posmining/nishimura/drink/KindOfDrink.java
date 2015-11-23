package posmining.nishimura.drink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

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
public class KindOfDrink {
	//一時的にスイーツを含むレシートIDを格納しておくためのリスト
	public static List<String> receipt_id = new ArrayList<String>();
	/*
	//本命データ（分類コード）
	public static List<String> sava_item_category = new ArrayList<String>();
	//本命データ（個数）
	public static List<Integer> save_item_count = new ArrayList<Integer>();
	 */
	//本命データ（分類コード、個数）
	public static TreeMap<String,Integer> save_item = new TreeMap<String,Integer>();

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(KindOfDrink.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015028");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/nishimura/result";     // ★MRの出力先

		if (args.length > 0) {
			inputpath = args[0];
		}

		//レシートIDを読み込み
		BufferedReader br = new BufferedReader(new FileReader("out/nishimura/drink/CheckReceiptId/part-r-00000"));
		String line = null;
		while ((line = br.readLine()) != null) {
			receipt_id.add(line);
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
	 *
	 *マッピングをここでは、リストにしている
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			//ドリンクでないものは無視する
			if(!PosUtils.isDrinkCode(csv[PosUtils.ITEM_CATEGORY_CODE])){
				return;
			}
			//同じレシートidでないデータは無視
			else if(receipt_id.indexOf(csv[PosUtils.RECEIPT_ID]) < 0){
				return;
			}

			String item_category_code = csv[PosUtils.ITEM_CATEGORY_CODE].substring(0, 3).toString();
			int item_count = Integer.parseInt(csv[PosUtils.ITEM_COUNT]);

			if(save_item.containsKey(item_category_code) == false){
				//新しいカテゴリー
				save_item.put(item_category_code, item_count);
			}
			else{
				//リスト内のデータを更新

				//ここがおかしい
				save_item.put(item_category_code,save_item.get(item_category_code) + item_count);
			}

			//文字列を無理やり連結してユニークなキーを作成
			String input_key = csv[PosUtils.RECEIPT_ID] +"_"+ csv[PosUtils.ITEM_NAME];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(input_key), new CSKV(0));
		}
	}

	/**
	 *  Reducerクラスのreduce関数を定義
	 *	このメソッドはkeyごとに一度実行される．
	 */
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			//リスト内のデータのみを入れる
			if(save_item.size() <= 0){
				return;
			}
			// emit(ドリンクの種類、個数)
			context.write(new CSKV(save_item.firstKey()), new CSKV(save_item.get(save_item.firstKey())));
			//入力したキーを削除
			save_item.remove(save_item.firstKey());
		}
	}
}