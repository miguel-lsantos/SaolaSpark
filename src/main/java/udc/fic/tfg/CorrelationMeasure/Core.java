package udc.fic.tfg.CorrelationMeasure;


public class Core {
			
	public static double h(double[] vector) {
        return Entropy.calculateEntropy(vector);
    }

    public static double h(double[][] vector) {
    	double [] merged = new double [vector.length*vector[0].length];;
        ArrayOperations.mergeMultipleArrays(vector, merged);

        return Entropy.calculateEntropy(merged);
    }
			
	public static double mi(double[] firstVector, double[] secondVector) {
        return MutualInformation.calculateMutualInformation(firstVector, secondVector);
    }

    public static double mi(double[][] firstVector, double[][] secondVector) {
    	double [] mergedFirst = new double [firstVector.length*firstVector[0].length];
    	double [] mergedSecond = new double[secondVector.length*secondVector[0].length];
        ArrayOperations.mergeMultipleArrays(firstVector, mergedFirst);
        ArrayOperations.mergeMultipleArrays(secondVector, mergedSecond);

        return MutualInformation.calculateMutualInformation(mergedFirst, mergedSecond);
    }
	
    public static double SU(double[] firstVector, double[] secondVector) {
        double hX = h(firstVector);
        double hY = h(secondVector);
        double iXY = mi(firstVector,secondVector);

        return (2 * iXY) / (hX + hY);
    }
}