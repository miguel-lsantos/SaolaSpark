package udc.fic.tfg;

import java.util.Date;
import java.util.List;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import udc.fic.tfg.CorrelationMeasure.Core;

import java.util.ArrayList;
import java.util.Arrays;

public class GroupSaola {

	public static int groupSaloa(int[][] groupFeature, double[][] data, int classAttribute, double threshold) {

		Date date = new Date();
		long startTime = date.getTime();
		RealMatrix matrix = MatrixUtils.createRealMatrix(data);

		int numGroup = groupFeature.length;
		int numFeatures = data[0].length;

		double[] dep = new double[numFeatures]; 
		Arrays.fill(dep, 0);

		int CI = 1;

		List<Integer>[] g = new List[numGroup];

		for(int i = 0; i<numGroup; i++){

			int[] f_g_index = groupFeature[i];
			ArrayList<Integer> currentFeature = new ArrayList<Integer>();
			for(int j = 0; i<f_g_index.length; j++) {

				double n1 = Arrays.stream(matrix.getColumn(f_g_index[j])).sum();
				if (n1==0)
					continue;

				dep[f_g_index[j]] = Core.SU(matrix.getColumn(f_g_index[j]),matrix.getColumn(classAttribute));

				if (dep[f_g_index[j]]<=threshold)
					continue;

				currentFeature.add(f_g_index[j]);
				ArrayList<Integer> currentFeature1 = currentFeature;
				currentFeature1.removeAll(new ArrayList<Integer>(Arrays.asList(f_g_index[j])));

				int p = currentFeature1.size();
				if (p>0) {

					for(int k = 0; i<p; k++){

						double dep_ij=Core.SU(matrix.getColumn(f_g_index[j]), matrix.getColumn(currentFeature1.get(k)));

						if (dep_ij<=threshold)
							continue;

						double t_dep=dep_ij;
						int t_feature = currentFeature1.get(k);

						if (dep[t_feature]>=dep[f_g_index[j]] && t_dep>Math.min(dep[f_g_index[j]],dep[t_feature])) {   
							currentFeature.removeAll(new ArrayList<Integer>(Arrays.asList(f_g_index[j])));
							break;   
						}


						if (dep[f_g_index[j]]>dep[t_feature] && t_dep>Math.min(dep[f_g_index[j]],dep[t_feature]))
							currentFeature.remove((Integer) t_feature);
					}
				}
			}//for j=1:length(f_g_index) 

			g[i]=currentFeature;
			if (!g[i].isEmpty()) {

				CI=1;

				for(int m=1;m<i-1;m++) {

					ArrayList<Integer> g1 = (ArrayList<Integer>) g[m];

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
			if(g[l].size()>0){
				selectFeature.addAll(g[l]);;
				selectedGroups++;
			}
		}
		long time = date.getTime() - startTime;
		return selectedGroups;
	}
}