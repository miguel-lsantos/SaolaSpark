package udc.fic.tfg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import udc.fic.tfg.CorrelationMeasure.Core;

public class Main 
{
    private static final String COMMA_DELIMITER = ",";

	public static void main( String[] args )
    {
		if (args.length != 4) {
			System.out.println("4 Argumentos Necesarios: file, rows, columns, threshold");
			return;
		}
		String file = args[0];
		int rows = Integer.parseInt(args[1]);
		int columns = Integer.parseInt(args[2]);
		double threshold = Double.parseDouble(args[3]);

    	RealMatrix data = MatrixUtils.createRealMatrix(rows, columns);
		System.out.println("Data Loading");
    	Date date = new Date();
    	long startTime = date.getTime();
    	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
    	    String line;
    	    int j=0;
    	    while ((line = br.readLine()) != null && (j < rows)) {
    	        String[] values = line.split(COMMA_DELIMITER);
    	        double[] numbers = new double[values.length];
    	        for(int i = 0; i < values.length; i++)
    	        {
    	           numbers[i] = Double.parseDouble(values[i]);
    	        }
				data.setRow(j, numbers);
    	        j++;
    	    }
    	} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("Data Loaded");
        
        int[] features = new int[data.getColumnDimension()-1];
        for(int i=1; i<data.getColumnDimension(); i++) {
        	features[i - 1] = i;
        }
        System.out.print("Features: ");
        System.out.println(Arrays.toString(features));
		System.out.print("Size: ");
		System.out.println(data.getColumnDimension());
        System.out.print("Rows: ");
        System.out.println(data.getRowDimension());
       
        
        System.out.println("SALOA");
        long time = new Date().getTime() - startTime;
		System.out.print("Read Time=");
		System.out.println(time);
        int numSelected = saola(features, data, 0, threshold);
        System.out.println(numSelected);
		time = new Date().getTime() - startTime;
		System.out.print("Total Time=");
		System.out.println(time);
    }
	
	public static int saola(int[] featureIndex, RealMatrix matrix, int classAttribute, double threshold) {
		Date date = new Date();
		long startTime = date.getTime();
		int numFeatures = matrix.getColumnDimension();

		double[] dep = new double[numFeatures];
		Arrays.fill(dep, 0);

		ArrayList<Integer> currentFeature = new ArrayList<Integer>();
		for(int j = 0; j<featureIndex.length; j++) {

			double n1 = Arrays.stream(matrix.getColumn(featureIndex[j])).sum();
			if (n1==0)
				continue;

			dep[featureIndex[j]] = Core.SU(matrix.getColumn(featureIndex[j]),matrix.getColumn(classAttribute));

			//REMOVE IRRELEVANT
			if (dep[featureIndex[j]]<=threshold) {
				continue;
			}
			ArrayList<Integer> currentFeature1 = new ArrayList<Integer>(currentFeature);
			currentFeature.add(featureIndex[j]);

			int p = currentFeature1.size();
			if (p>0) {

				for(int k = 0; k<p; k++) {

					double dep_ij = Core.SU(matrix.getColumn(featureIndex[j]), matrix.getColumn(currentFeature1.get(k)));
					if (dep_ij <= threshold)
						continue;

					double t_dep = dep_ij;
					int t_feature = currentFeature1.get(k);

					//REMOVE 1ST REDUNDANT
					if ((dep[t_feature] > dep[featureIndex[j]]) && (t_dep > dep[featureIndex[j]])) {
						currentFeature.remove((Integer) featureIndex[j]);
						break;
					}

					//REMOVE 2ST REDUNDANT
					if ((dep[featureIndex[j]] > dep[t_feature]) && (t_dep > dep[t_feature])){
						currentFeature.remove((Integer) t_feature);
					}
				}
			}
		}
		System.out.println();
		System.out.println(currentFeature.toString());
		long time = new Date().getTime() - startTime;
		System.out.print("Time=");
		System.out.println(time);
		return currentFeature.size();
	}
	
	
	public static int groupSaloa(int[][] groupFeature, double[][] data, int classAttribute, double threshold) {
		System.out.print(data.length);
		Date date = new Date();
		long startTime = date.getTime();
		RealMatrix matrix = MatrixUtils.createRealMatrix(data);

		int numGroup = groupFeature.length;
		int numFeatures = data[0].length;

		double[] dep = new double[numFeatures-1];
		Arrays.fill(dep, 0);
		
		List<Integer>[] g = new List[numGroup];

		for(int i = 0; i<numGroup; i++){

			int[] f_g_index = groupFeature[i];
			ArrayList<Integer> currentFeature = new ArrayList<Integer>();
			for(int j = 0; j<f_g_index.length; j++) {

				double n1 = Arrays.stream(matrix.getColumn(f_g_index[j])).sum();
				if (n1==0)
					continue;
				dep[f_g_index[j]] = Core.SU(matrix.getColumn(f_g_index[j]),matrix.getColumn(classAttribute));
				
				if (dep[f_g_index[j]]<=threshold)
					continue;
				
				ArrayList<Integer> currentFeature1 = new ArrayList<Integer>(currentFeature);
				currentFeature.add(f_g_index[j]);

				int p = currentFeature1.size();
				if (p>0) {

					for(int k = 0; k<p; k++){

						double dep_ij=Core.SU(matrix.getColumn(f_g_index[j]), matrix.getColumn(currentFeature1.get(k)));

						if (dep_ij<=threshold)
							continue;

						double t_dep=dep_ij;
						int t_feature = currentFeature1.get(k);

						if (dep[t_feature]>=dep[f_g_index[j]] && t_dep>Math.min(dep[f_g_index[j]],dep[t_feature])) {   
							currentFeature.remove((Integer) f_g_index[j]);
							break;   
						}


						if (dep[f_g_index[j]]>dep[t_feature] && t_dep>Math.min(dep[f_g_index[j]],dep[t_feature]))
							currentFeature.remove((Integer) t_feature);
					}
				}
			}//for j=1:length(f_g_index) 
			g[i]=currentFeature;
			if (!g[i].isEmpty()) {
				for(int m=0;m<i;m++) {

					ArrayList<Integer> g1 = new ArrayList<Integer>(g[m]);

					for (int m1=0;m1<currentFeature.size();m1++) {
						for (int m2=0;m2<g1.size(); m2++) {

							double dep_ij1=Core.SU(matrix.getColumn(g1.get(m2)),matrix.getColumn(currentFeature.get(m1)));
							if (dep_ij1<=threshold)
								continue;


							double t_dep1=dep_ij1;
							int t_feature1 = currentFeature.get(m1);
							if (dep[g1.get(m2)]>dep[t_feature1] && t_dep1>Math.min(dep[g1.get(m2)],dep[t_feature1])) {
								g[i].remove((Integer) t_feature1);
								break;  
							}
							if (dep[t_feature1]>=dep[g1.get(m2)] && t_dep1>Math.min(dep[g1.get(m2)], dep[t_feature1]))
								g[m].remove((Integer) g1.get(m2));
						}
						
					}
				}
			}
		}
		ArrayList<Integer> selectFeature = new ArrayList<Integer>();
		int selectedGroups = 0;
		for(int l=0; l<numGroup; l++){
			if (g[l] != null) {
				if(g[l].size()>0){
					selectFeature.addAll(g[l]);;
					selectedGroups++;
				}
			}
		}
		System.out.println("Selected:" + selectFeature.toString());
		long time = new Date().getTime() - startTime;
		System.out.println("Time=" + time);
		return selectedGroups;
	}

}
