package udc.fic.tfg.CorrelationMeasure.States;

import java.util.HashMap;

import udc.fic.tfg.CorrelationMeasure.ArrayOperations;

public class ProbabilityState
{
  public final HashMap<Integer,Double> probMap;
  public final int maxState;
  
  /**
   * Constructor for the ProbabilityState class. Takes a data vector and calculates
   * the marginal probability of each state, storing each state/probability pair in a HashMap.
   *
   * @param  dataVector  Input vector. It is discretised to the floor of each value.
   */
  public ProbabilityState(double[] dataVector)
  {
    probMap = new HashMap<Integer,Double>();
    int vectorLength = dataVector.length;
    double doubleLength = dataVector.length;

    //round input to integers
    int[] normalisedVector = new int[vectorLength];
    maxState = ArrayOperations.normaliseArray(dataVector,normalisedVector);
   
    HashMap<Integer,Integer> countMap = new HashMap<Integer,Integer>();

    for (int value : normalisedVector)
    {
        Integer tmpKey = value;
        Integer tmpValue = countMap.remove(tmpKey);
        if (tmpValue == null)
        {
            countMap.put(tmpKey,1);
        }
        else
        {
            countMap.put(tmpKey,tmpValue + 1);
        }
    }

    for (Integer key : countMap.keySet())
    {
        probMap.put(key,countMap.get(key) / doubleLength);
    }
  }//constructor(double[])
}