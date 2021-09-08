package udc.fic.tfg.CorrelationMeasure;

import udc.fic.tfg.CorrelationMeasure.States.*;

public class Entropy {
	
	public static double calculateEntropy(double[] dataVector)
	  {
	    ProbabilityState state = new ProbabilityState(dataVector);

	    double entropy = 0.0;
	    for (Double prob : state.probMap.values())
	    {
	      if (prob > 0) 
	      {
	        entropy -= prob * Math.log(prob);
	      }
	    }

	    entropy /= Math.log(2);
	    
	    return entropy;
	  }//calculateEntropy(double [])
	
	public static double calculateEntropyApache(double[] dataVector)
	  {
	    ProbabilityState state = new ProbabilityState(dataVector);

	    double entropy = 0.0;
	    for (Double prob : state.probMap.values())
	    {
	      if (prob > 0) 
	      {
	        entropy -= prob * Math.log(prob);
	      }
	    }

	    entropy /= Math.log(2);
	    
	    return entropy;
	  }//calculateEntropy(double [])
	/*
    public static double calculateEntropy(double[] dataVector, int vectorLength){
        double entropy = 0.0;
        double tempValue = 0.0;
        int i;
        ProbabilityState state = CalculateProbability.calculateProbability(dataVector,vectorLength);
        
        /*H(X) = - sum p(x) log p(x)
        for (i = 0; i < state.numStates; i++){
            tempValue = state.probabilityVector[i];
            if (tempValue > 0){
                entropy -= tempValue * Math.log(tempValue);
            }
        }
        
        entropy /= Math.log(2.0);
        
        return entropy;
    }calculateEntropy(double *,int)

    public static double calculateJointEntropy(double[] firstVector, double[] secondVector, int vectorLength){
        double jointEntropy = 0.0;  
        double tempValue = 0.0;
        int i;
        JointProbabilityState state = CalculateProbability.calculateJointProbability(firstVector,secondVector,vectorLength);
        
        /*H(XY) = - sumx sumy p(xy) log p(xy)
        for (i = 0; i < state.numJointStates; i++){
            tempValue = state.jointProbabilityVector[i];
            if (tempValue > 0){
                jointEntropy -= tempValue * Math.log(tempValue);
            }
        }
        
        jointEntropy /= Math.log(2.0);
        
        return jointEntropy;
    }/*calculateJointEntropy(double *, double *, int)

    public static double calculateConditionalEntropy(double[] dataVector, double[] conditionVector, int vectorLength){
        /*
        ** Conditional entropy
        ** H(X|Y) = - sumx sumy p(xy) log p(xy)/p(y)
        
        
        double condEntropy = 0.0;  
        double jointValue = 0.0;
        double condValue = 0.0;
        int i;
        JointProbabilityState state = CalculateProbability.calculateJointProbability(dataVector,conditionVector,vectorLength);
        
        /*H(X|Y) = - sumx sumy p(xy) log p(xy)/p(y)*/
        /* to index by numFirstStates use modulus of i
        ** to index by numSecondStates use integer division of i by numFirstStates
        
        for (i = 0; i < state.numJointStates; i++){
            jointValue = state.jointProbabilityVector[i];
            condValue = state.secondProbabilityVector[i / state.numFirstStates];
            if ((jointValue > 0) && (condValue > 0)){
                condEntropy -= jointValue * Math.log(jointValue / condValue);
            }
        }
        
        condEntropy /= Math.log(2.0);
        
        return condEntropy;

    }/*calculateConditionalEntropy(double *, double *, int)*/
}