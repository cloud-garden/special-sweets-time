package posmining.utils.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;



public class ArrangeTSVFile {

	public void exportHeatmapData(String fileDir, String fileName ,String outFileName){
		File inputFile = new File(fileDir,fileName);
		File outFile = new File(fileDir , outFileName);

		try {
			outFile.createNewFile();
			PrintWriter writer = new PrintWriter(outFile);

			for(int i = 0 ; i < 24 ; i++){
				writer.print(i);
				if(i < 23)
					writer.print(",");
				else
					writer.println();
			}

			FileReader reader = new FileReader(inputFile);
			BufferedReader br = new BufferedReader(reader);
			String line = br.readLine() ;

			while(line != null){
				String[] strings = line.split("\t");

				writer.print(strings[2]);
				if(strings[1].contains("23")){
					writer.println();
				}else{
					writer.print(",");
				}
				line = br.readLine();
			}

			writer.close();
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
