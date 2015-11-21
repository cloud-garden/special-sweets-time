package posmining.nishimura.drink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
	//本命データ（分類コード）
	public static List<String> sava_item_category = new ArrayList<String>();
	//本命データ（個数）
	public static List<Integer> save_item_count = new ArrayList<Integer>();


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
		String outputpath = "out/nishimura/result/";     // ★MRの出力先

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
			else if(!compare_list(csv[PosUtils.RECEIPT_ID])){
				System.out.println("false : "+csv[PosUtils.ITEM_NAME]);
				return;
			}

			System.out.println("true");


			String item_category_code = csv[PosUtils.ITEM_CATEGORY_CODE].substring(0, 3);
			String item_count = csv[PosUtils.ITEM_COUNT];

			int target_number = target_number(csv[PosUtils.ITEM_CATEGORY_CODE]);


			//System.out.println(csv[PosUtils.ITEM_NAME]);


			if(target_number<0){
				//新しいカテゴリー
				sava_item_category.add(item_category_code);
				save_item_count.add(Integer.valueOf(item_count).intValue());
			}
			else{
				//リスト内のデータを更新
				save_item_count.set(target_number,save_item_count.get(target_number)+Integer.valueOf(item_count).intValue());
			}
			return ;
			//System.out.println(csv[PosUtils.ITEM_NAME]);

			//文字列を無理やり連結してユニークなキーを作成
			//String input_key = csv[PosUtils.RECEIPT_ID] +"_"+ item_category_code +"_"+ item_count;

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			//context.write(new CSKV(input_key), new CSKV(0));
		}
	}

	/**
	 *  Reducerクラスのreduce関数を定義
	 *	このメソッドはkeyごとに一度実行される．
	 */
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		int i=-1;
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			i++;
			//リスト内のデータのみを入れる
			if(i < sava_item_category.size()){
				return;
			}

			// emit(ドリンクの種類、個数)
			context.write(new CSKV(sava_item_category.get(i)), new CSKV(save_item_count.get(i)));
		}
	}


	/*
	 * 同一のレシートIDがあるかを確認
	 */
	static boolean compare_list(String target){
		//同じレシートidでないデータは無視
		for(int i = 0; i < receipt_id.size() ;i++){
			//リスト内の全レシートIDと比較
			if(target==receipt_id.get(i)){
				return true;
			}
		}
		return false;
	}
	/*
	 *targetデータがリスト内にあるかを確認
	 *なければ、－１を返す
	 */
	static int target_number(String target){

		//同じitem_categoryでないデータは無視
		for(int i = 0; i < sava_item_category.size() ;i++){
			//リスト内の全リストと比較
			if(target==sava_item_category.get(i)){
				return i;
			}
		}
		//なければ、-1を返す
		return -1;
	}
}