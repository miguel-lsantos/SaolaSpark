package udc.fic.tfg.CorrelationMeasure;


public class CalculateProbability {/*
    public static JointProbabilityState calculateJointProbability(double[] firstVector, double[] secondVector, int vectorLength){
        int firstNumStates;
        int secondNumStates;
        int jointNumStates;
        int i;
        double length = vectorLength;

        int[] firstNormalisedVector = new int[vectorLength];
        int[] secondNormalisedVector = new int[vectorLength];
        
        firstNumStates = ArrayOperations.normaliseArray(firstVector,firstNormalisedVector);
        secondNumStates = ArrayOperations.normaliseArray(secondVector,secondNormalisedVector);
        jointNumStates = firstNumStates * secondNumStates;

        int[] firstStateCounts = new int[firstNumStates];
        int[] secondStateCounts = new int[secondNumStates];
        int[] jointStateCounts = new int[jointNumStates];

        double[] firstStateProbs = new double[firstNumStates];
        double[] secondStateProbs = new double[secondNumStates];
        double[] jointStateProbs = new double[jointNumStates];
            
        for (i = 0; i < vectorLength; i++){
            firstStateCounts[firstNormalisedVector[i]] += 1;
            secondStateCounts[secondNormalisedVector[i]] += 1;
            jointStateCounts[secondNormalisedVector[i] * firstNumStates + firstNormalisedVector[i]] += 1;
        }
        
        for (i = 0; i < firstNumStates; i++){
            firstStateProbs[i] = firstStateCounts[i] / length;
        }
        
        for (i = 0; i < secondNumStates; i++){
            secondStateProbs[i] = secondStateCounts[i] / length;
        }
        
        for (i = 0; i < jointNumStates; i++){
            jointStateProbs[i] = jointStateCounts[i] / length;
        }

        JointProbabilityState state = new JointProbabilityState();
        state.jointProbabilityVector = jointStateProbs;
        state.numJointStates = jointNumStates;
        state.firstProbabilityVector = firstStateProbs;
        state.numFirstStates = firstNumStates;
        state.secondProbabilityVector = secondStateProbs;
        state.numSecondStates = secondNumStates;

        return state;
    }

    public static WeightedJointProbState calculateWeightedJointProbability(double[] firstVector, double[] secondVector,
            double[] weightVector, int vectorLength) {
        int firstNumStates;
        int secondNumStates;
        int jointNumStates;
        int i;
        double length = vectorLength;

        int[] firstNormalisedVector = new int[vectorLength];
        int[] secondNormalisedVector = new int[vectorLength];

        firstNumStates = ArrayOperations.normaliseArray(firstVector, firstNormalisedVector);
        secondNumStates = ArrayOperations.normaliseArray(secondVector, secondNormalisedVector);
        jointNumStates = firstNumStates * secondNumStates;

        int[] firstStateCounts = new int[firstNumStates];
        int[] secondStateCounts = new int[secondNumStates];
        int[] jointStateCounts = new int[jointNumStates];

        double[] firstStateProbs = new double[firstNumStates];
        double[] secondStateProbs = new double[secondNumStates];
        double[] jointStateProbs = new double[jointNumStates];

        double[] firstWeightVec = new double[firstNumStates];
        double[] secondWeightVec = new double[secondNumStates];
        double[] jointWeightVec = new double[jointNumStates];

        for (i = 0; i < vectorLength; i++) {
            firstStateCounts[firstNormalisedVector[i]] += 1;
            secondStateCounts[secondNormalisedVector[i]] += 1;
            jointStateCounts[secondNormalisedVector[i] * firstNumStates + firstNormalisedVector[i]] += 1;

            firstWeightVec[firstNormalisedVector[i]] += weightVector[i];
            secondWeightVec[secondNormalisedVector[i]] += weightVector[i];
            jointWeightVec[secondNormalisedVector[i] * firstNumStates + firstNormalisedVector[i]] += weightVector[i];
        }

        for (i = 0; i < firstNumStates; i++) {
            if (firstStateCounts[i] == 1) {
                firstStateProbs[i] = firstStateCounts[i] / length;
                firstWeightVec[i] /= firstStateCounts[i];
            }
        }

        for (i = 0; i < secondNumStates; i++) {
            if (secondStateCounts[i] == 1) {
                secondStateProbs[i] = secondStateCounts[i] / length;
                secondWeightVec[i] /= secondStateCounts[i];
            }
        }

        for (i = 0; i < jointNumStates; i++) {
            if (jointStateCounts[i] == 1) {
                jointStateProbs[i] = jointStateCounts[i] / length;
                jointWeightVec[i] /= jointStateCounts[i];
            }
        }

        WeightedJointProbState state = new WeightedJointProbState();

        state.jointProbabilityVector = jointStateProbs;
        state.jointWeightVector = jointWeightVec;
        state.numJointStates = jointNumStates;
        state.firstProbabilityVector = firstStateProbs;
        state.firstWeightVector = firstWeightVec;
        state.numFirstStates = firstNumStates;
        state.secondProbabilityVector = secondStateProbs;
        state.secondWeightVector = secondWeightVec;
        state.numSecondStates = secondNumStates;

        return state;
    }

    public static ProbabilityState calculateProbability(double[] dataVector, int vectorLength){
        int numStates;
        int i;
        double length = vectorLength;

        int[] normalisedVector = new int[vectorLength];
        
        numStates = ArrayOperations.normaliseArray(dataVector,normalisedVector);
        
        int[] stateCounts = new int[numStates];
        double[] stateProbs =new double[numStates];

        for (i = 0; i < vectorLength; i++){
            stateCounts[normalisedVector[i]] += 1;
        }
        
        for (i = 0; i < numStates; i++){
            stateProbs[i] = stateCounts[i] / length;
        }
        
        ProbabilityState state = new ProbabilityState();
        state.probabilityVector = stateProbs;
        state.numStates = numStates;

        return state;
    }/* calculateProbability(double *,int) 

    public static WeightedProbState calculateWeightedProbability(double[] dataVector, double[] exampleWeightVector, int vectorLength){
        int numStates;
        int i;
        double length = vectorLength;

        int[] normalisedVector = new int[vectorLength];
        
        numStates = ArrayOperations.normaliseArray(dataVector,normalisedVector);
        int[] stateCounts = new int[numStates];
        double[] stateProbs = new double[numStates];
        double[] stateWeights = new double[numStates];

        for (i = 0; i < vectorLength; i++){
            stateCounts[normalisedVector[i]] += 1;
            stateWeights[normalisedVector[i]] += exampleWeightVector[i];
        }
        
        for (i = 0; i < numStates; i++){
            stateProbs[i] = stateCounts[i] / length;
            stateWeights[i] /= stateCounts[i];
        }
        
        WeightedProbState state = new WeightedProbState();
        state.probabilityVector = stateProbs;
        state.stateWeightVector = stateWeights;
        state.numStates = numStates;

        return state;
    }*/
}