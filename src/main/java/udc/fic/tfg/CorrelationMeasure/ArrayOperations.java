package udc.fic.tfg.CorrelationMeasure;

public class ArrayOperations {

    public static void incrementVector(double[] vector, int vectorLength) {
        int i = 0;
        for (i = 0; i < vectorLength; i++){
            vector[i]++; /*C indexes from 0 not 1*/
        }/*for length of array */
    }/* incrementVector(double*,int) */

    public static void printDoubleVector(double[] vector, int vectorLength){
        int i;
        for (i = 0; i < vectorLength; i++){
            if (vector[i] > 0)
            System.out.println("Val at i=" + i + ", is " + vector[i] + "\n");
        }/*for number of items in vector*/
    }/*printDoubleVector(double*,int)*/

    public static void printIntVector(int[] vector, int vectorLength){
        int i;
        for (i = 0; i < vectorLength; i++){
            System.out.println("Val at i=" + i + ", is " + vector[i] + "\n");
        }/*for number of items in vector*/
    }/*printIntVector(int*,int)*/

    public static int numberOfUniqueValues(double[] featureVector, int vectorLength){
        int uniqueValues = 0;
        double[] valuesArray = new double[vectorLength];
        
        boolean found = false;
        int j = 0;
        int i;
            
        for (i = 0; i < vectorLength; i++){
            found = false;
            j = 0;
            while ((j < uniqueValues) && (!found)){
                if (valuesArray[j] == featureVector[i]){
                    found = true;
                    featureVector[i] = (double) (j+1);
                }
                j++;
            }
            if (!found){
                valuesArray[uniqueValues] = featureVector[i];
                uniqueValues++;
                featureVector[i] = (double) uniqueValues;
            }
        }/*for vectorlength*/
        
        return uniqueValues;
    }/*numberOfUniqueValues(double*,int)*/

    /******************************************************************************* 
    ** normaliseArray takes an input vector and writes an output vector
    ** which is a normalised version of the input, and returns the number of states
    ** A normalised array has min value = 0, max value = number of states
    ** and all values are integers
    **
    *******************************************************************************/
    public static final int normaliseArray(double[] inputVector, int[] outputVector)
    {
      int minVal = 0;
      int maxVal = 0;
      int currentValue;
      int i;
      int vectorLength = inputVector.length;
      
      if (vectorLength > 0)
      {
        minVal = (int) Math.floor(inputVector[0]);
        maxVal = (int) Math.floor(inputVector[0]);
      
        for (i = 0; i < vectorLength; i++)
        {
          currentValue = (int) Math.floor(inputVector[i]);
          outputVector[i] = currentValue;
          
          if (currentValue < minVal)
          {
            minVal = currentValue;
          }
          
          if (currentValue > maxVal)
          {
            maxVal = currentValue;
          }
        }/*for loop over vector*/
        
        for (i = 0; i < vectorLength; i++)
        {
          outputVector[i] = outputVector[i] - minVal;
        }

        maxVal = (maxVal - minVal) + 1;
      }

      return maxVal;
    }//normaliseArray(double[],double[])


    /*******************************************************************************
    ** mergeArrays takes in two arrays and writes the joint state of those arrays
    ** to the output vector, returning the number of joint states
    **
    ** the length of the vectors must be the same and equal to vectorLength
    ** outputVector must be malloc'd before calling this function
    *******************************************************************************/
    public static final int mergeArrays(double[] firstVector, double[] secondVector, double[] outputVector)
    {
      int[] firstNormalisedVector;
      int[] secondNormalisedVector;
      int firstNumStates;
      int secondNumStates;
      int i;
      int[] stateMap;
      int stateCount;
      int curIndex;
      int vectorLength = firstVector.length;
      
      firstNormalisedVector = new int[vectorLength];
      secondNormalisedVector = new int[vectorLength];

      firstNumStates = normaliseArray(firstVector,firstNormalisedVector);
      secondNumStates = normaliseArray(secondVector,secondNormalisedVector);
      
      stateMap = new int[firstNumStates*secondNumStates];
      stateCount = 1;
      for (i = 0; i < vectorLength; i++)
      {
        curIndex = firstNormalisedVector[i] + (secondNormalisedVector[i] * firstNumStates);
        if (stateMap[curIndex] == 0)
        {
          stateMap[curIndex] = stateCount;
          stateCount++;
        }
        outputVector[i] = stateMap[curIndex];
      }
        
      return stateCount;
    }//mergeArrays(double[],double[],double[])

    public static int mergeArraysArities(double[] firstVector, int numFirstStates, double[] secondVector, int numSecondStates, double[] outputVector, int vectorLength){
        int i;
        int totalStates;
        int firstStateCheck, secondStateCheck;
        
        int[] firstNormalisedVector = new int[vectorLength];
        int[] secondNormalisedVector = new int[vectorLength];

        firstStateCheck = normaliseArray(firstVector,firstNormalisedVector);
        secondStateCheck = normaliseArray(secondVector,secondNormalisedVector);
        
        if ((firstStateCheck <= numFirstStates) && (secondStateCheck <= numSecondStates)){
            for (i = 0; i < vectorLength; i++){
                outputVector[i] = firstNormalisedVector[i] + (secondNormalisedVector[i] * numFirstStates) + 1;
            }
            totalStates = numFirstStates * numSecondStates;
        }
        else{
            totalStates = -1;
        }
        
        return totalStates;
    }/*mergeArraysArities(double *,int,double *,int,double *,int)*/

    public static int mergeMultipleArrays(double[][] inputMatrix, double[] outputVector){
    	int matrixWidth = inputMatrix.length;
    	int vectorLength = inputMatrix[0].length;
        int i = 0;
        int currentNumStates;
        
        if (matrixWidth > 1){
            currentNumStates = mergeArrays(inputMatrix[0], inputMatrix[0], outputVector);
            for (i = 1; i < matrixWidth; i++){
                currentNumStates = mergeArrays(outputVector,inputMatrix[i], outputVector);
            }
        }
        else{
            int[] normalisedVector = new int[vectorLength];
            currentNumStates = normaliseArray(inputMatrix[0],normalisedVector);
            for (i = 0; i < vectorLength; i++){
                outputVector[i] = inputMatrix[0][i];
            }
        }
        
        return currentNumStates;
    }/*mergeMultipleArrays(double *, double *, int, int, bool)*/


    public static int mergeMultipleArraysArities(double[][] inputMatrix, double[] outputVector, int matrixWidth, int[] arities, int vectorLength){
        int i = 0;
        int currentNumStates;
        
        if (matrixWidth > 1){
            currentNumStates = mergeArraysArities(inputMatrix[0], arities[0], inputMatrix[0], arities[1], outputVector,vectorLength);
            for (i = 1; i < matrixWidth; i++){
                currentNumStates = mergeArraysArities(outputVector,currentNumStates, inputMatrix[i],arities[i],outputVector,vectorLength);
                if (currentNumStates == -1) break;
            }
        }
        else{ 
            int[] normalisedVector = new int[vectorLength];
            currentNumStates = normaliseArray(inputMatrix[0],normalisedVector);
            for (i = 0; i < vectorLength; i++){
                outputVector[i] = inputMatrix[0][i];
            }
        }
        
        return currentNumStates;
    }
}