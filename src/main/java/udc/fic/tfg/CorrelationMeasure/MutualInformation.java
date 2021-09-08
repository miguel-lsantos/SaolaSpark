package udc.fic.tfg.CorrelationMeasure;

import udc.fic.tfg.CorrelationMeasure.States.*;

public class MutualInformation {
	
	public strictfp static double calculateMutualInformation(double[] firstVector, double[] secondVector)
	  {
	    JointProbabilityState state = new JointProbabilityState(firstVector,secondVector);

	    int numFirstStates = state.firstMaxVal;
	    double jointValue, firstValue, secondValue;
	    double mutualInformation = 0.0;
	    for (Integer key : state.jointProbMap.keySet())
	    {
	      jointValue = state.jointProbMap.get(key);
	      firstValue = state.firstProbMap.get(key % numFirstStates);
	      secondValue = state.secondProbMap.get(key / numFirstStates);

	      if ((jointValue > 0) && (firstValue > 0) && (secondValue > 0))
	      {
	    	double aux = jointValue * Math.log(jointValue / firstValue / secondValue);
	        mutualInformation += aux;
	      }
	    }
	    mutualInformation /= Math.log(2);
	    return mutualInformation; 
	  }//calculateMutualInformation(double [], double [])
	/*
	public static double calculateMutualInformation(double[] dataVector, double[] targetVector, int vectorLength){
        double mutualInformation = 0.0;
        int firstIndex,secondIndex;
        int i;
        JointProbabilityState state = CalculateProbability.calculateJointProbability(dataVector,targetVector,vectorLength);
            
        /*
        ** I(X;Y) = sum sum p(xy) * log (p(xy)/p(x)p(y))
        
        for (i = 0; i < state.numJointStates; i++){
            firstIndex = i % state.numFirstStates;
            secondIndex = i / state.numFirstStates;
            
            if ((state.jointProbabilityVector[i] > 0) && (state.firstProbabilityVector[firstIndex] > 0) && (state.secondProbabilityVector[secondIndex] > 0)){
            /*double division is probably more stable than multiplying two small numbers together
            ** mutualInformation += state.jointProbabilityVector[i] * log(state.jointProbabilityVector[i] / (state.firstProbabilityVector[firstIndex] * state.secondProbabilityVector[secondIndex]));
            
                mutualInformation += state.jointProbabilityVector[i] * Math.log(state.jointProbabilityVector[i] / state.firstProbabilityVector[firstIndex] / state.secondProbabilityVector[secondIndex]);
            }
        }
        
        mutualInformation /= Math.log(2.0);
        
        return mutualInformation;
    }/*calculateMutualInformation(double *,double *,int)

    public static double calculateConditionalMutualInformation(double[] dataVector, double[] targetVector, double[] conditionVector, int vectorLength){
        double mutualInformation = 0.0;
        double firstCondition, secondCondition;
        double[] mergedVector = new double[vectorLength];
        
        ArrayOperations.mergeArrays(targetVector,conditionVector,mergedVector);
        
        /* I(X;Y|Z) = H(X|Z) - H(X|YZ) 
        /* double calculateConditionalEntropy(double *dataVector, double *conditionVector, int vectorLength);
        firstCondition = Entropy.calculateConditionalEntropy(dataVector,conditionVector,vectorLength);
        secondCondition = Entropy.calculateConditionalEntropy(dataVector,mergedVector,vectorLength);
        
        mutualInformation = firstCondition - secondCondition;
        
        return mutualInformation;
    }/*calculateConditionalMutualInformation(double *,double *,double *,int)*/
}