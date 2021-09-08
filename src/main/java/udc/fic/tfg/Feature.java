package udc.fic.tfg;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Feature implements Serializable {
    private short[] featureVector;
    private int index;
    private int maxNumber;
    private int minNumber;
    private double entropy;
    private double classSU;


    public Feature() {
        this.index = -1;
    }

    public Feature(short[] featureVector, int index, int maxNumber, int minNumber) {
        this.featureVector = featureVector;
        this.index = index;
        this.maxNumber = maxNumber;
        this.minNumber = minNumber;
        this.entropy = -1;
        this.classSU = -1;
    }

    public short[] getFeatureVector() {
        return featureVector;
    }

    public void setFeatureVector(short[] featureVector) {
        this.featureVector = featureVector;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getMinNumber() {
        return minNumber;
    }

    public void setMinNumber(int minNumber) {
        this.minNumber = minNumber;
    }

    public int getMaxNumber() {
        return maxNumber;
    }

    public void setMaxNumber(int maxNumber) {
        this.maxNumber = maxNumber;
    }

    public double getEntropy() {
        return entropy;
    }

    public void setEntropy(double entropy) {
        this.entropy = entropy;
    }

    public double getClassSU() {
        return classSU;
    }

    public void setClassSU(double classSU) {
        this.classSU = classSU;
    }

    public void calculateEntropy(double doubleRows){
        Integer tmpKey, tmpValue;
        HashMap<Integer,Integer> countMap = new HashMap<Integer,Integer>();

        for (int i = 0; i < (int) doubleRows; i++)
        {
            tmpKey = this.featureVector[i] - this.minNumber;
            this.featureVector[i] = tmpKey.shortValue();
            tmpValue = countMap.remove(tmpKey);
            if (tmpValue == null)
            {
                countMap.put(tmpKey,1);
            }
            else
            {
                countMap.put(tmpKey,tmpValue + 1);
            }
        }
        this.minNumber = 0;

        double entropy = 0.0;
        double prob;
        for (Integer count : countMap.values())
        {
            prob = count / doubleRows;
            if (prob > 0)
            {
                entropy -= prob * Math.log(prob);
            }
        }

        entropy /= Math.log(2);
        this.entropy = entropy;
    }

    public double SU(Feature feature, double doubleRows) {
        int firstVal, secondVal, jointVal;
        Integer tmpKey, tmpValue;

        int firstMaxVal = this.getMaxNumber();

        HashMap<Integer,Integer> jointCountMap = new HashMap<Integer,Integer>();
        HashMap<Integer,Integer> firstCountMap = new HashMap<Integer,Integer>();
        HashMap<Integer,Integer> secondCountMap = new HashMap<Integer,Integer>();

        for (int i = 0; i < (int) doubleRows; i++)
        {
            firstVal = this.featureVector[i];

            secondVal = feature.featureVector[i];

            jointVal = firstVal + (firstMaxVal * secondVal);

            tmpKey = jointVal;
            tmpValue = jointCountMap.remove(tmpKey);
            if (tmpValue == null)
            {
                jointCountMap.put(tmpKey,1);
            }
            else
            {
                jointCountMap.put(tmpKey,tmpValue + 1);
            }

            tmpKey = firstVal;
            tmpValue = firstCountMap.remove(tmpKey);
            if (tmpValue == null)
            {
                firstCountMap.put(tmpKey,1);
            }
            else
            {
                firstCountMap.put(tmpKey,tmpValue + 1);
            }

            tmpKey = secondVal;
            tmpValue = secondCountMap.remove(tmpKey);
            if (tmpValue == null)
            {
                secondCountMap.put(tmpKey,1);
            }
            else
            {
                secondCountMap.put(tmpKey,tmpValue + 1);
            }
        }

        double jointValue, firstValue, secondValue;

        double mutualInformation = 0.0;
        for (Integer key : jointCountMap.keySet())
        {
            jointValue = jointCountMap.get(key) / doubleRows;
            firstValue = firstCountMap.get(key % firstMaxVal) / doubleRows;
            secondValue = secondCountMap.get(key / firstMaxVal) / doubleRows;

            if ((jointValue > 0) && (firstValue > 0) && (secondValue > 0))
            {
                double aux = jointValue * Math.log(jointValue / firstValue / secondValue);
                mutualInformation += aux;
            }
        }
        mutualInformation /= Math.log(2);

        return (2 * mutualInformation) / (this.getEntropy() + feature.getEntropy());
    }

    public void classSU(Feature classFeature, double doubleRows) {
        if (this.getIndex() < 0)
            return;
        int firstVal, secondVal, jointVal;
        Integer tmpKey, tmpValue;

        int firstMaxVal = this.getMaxNumber();

        HashMap<Integer,Integer> jointCountMap = new HashMap<Integer,Integer>();
        HashMap<Integer,Integer> firstCountMap = new HashMap<Integer,Integer>();
        HashMap<Integer,Integer> secondCountMap = new HashMap<Integer,Integer>();

        for (int i = 0; i < (int) doubleRows; i++)
        {
            firstVal = this.featureVector[i] - this.minNumber;
            this.featureVector[i] = (short) firstVal;

            secondVal = classFeature.featureVector[i];

            jointVal = firstVal + (firstMaxVal * secondVal);

            tmpKey = jointVal;
            tmpValue = jointCountMap.remove(tmpKey);
            if (tmpValue == null)
            {
                jointCountMap.put(tmpKey,1);
            }
            else
            {
                jointCountMap.put(tmpKey,tmpValue + 1);
            }

            tmpKey = firstVal;
            tmpValue = firstCountMap.remove(tmpKey);
            if (tmpValue == null)
            {
                firstCountMap.put(tmpKey,1);
            }
            else
            {
                firstCountMap.put(tmpKey,tmpValue + 1);
            }

            tmpKey = secondVal;
            tmpValue = secondCountMap.remove(tmpKey);
            if (tmpValue == null)
            {
                secondCountMap.put(tmpKey,1);
            }
            else
            {
                secondCountMap.put(tmpKey,tmpValue + 1);
            }
        }
        this.minNumber = 0;

        double jointValue, firstValue, secondValue;

        double mutualInformation = 0.0;
        for (Integer key : jointCountMap.keySet())
        {
            jointValue = jointCountMap.get(key) / doubleRows;
            firstValue = firstCountMap.get(key % firstMaxVal) / doubleRows;
            secondValue = secondCountMap.get(key / firstMaxVal) / doubleRows;

            if ((jointValue > 0) && (firstValue > 0) && (secondValue > 0))
            {
                double aux = jointValue * Math.log(jointValue / firstValue / secondValue);
                mutualInformation += aux;
            }
        }
        mutualInformation /= Math.log(2);


        double entropy = 0.0;
        double prob;
        for (Integer count : firstCountMap.values())
        {
            prob = count / doubleRows;
            if (prob > 0)
            {
                entropy -= prob * Math.log(prob);
            }
        }

        entropy /= Math.log(2);
        this.entropy = entropy;
        this.classSU = (2 * mutualInformation) / (entropy + classFeature.getEntropy());
    }

    public static List<Feature> addImportantFeatures(List<Feature> featureList, Feature feature, double doubleRows, double threshold) {

        double featureClassMU = feature.getClassSU();

        Iterator<Feature> itr = featureList.iterator();
        while(itr.hasNext()){
            Feature listFeature = itr.next();
            double betweenMutualInformation = feature.SU(listFeature, doubleRows);
            if (betweenMutualInformation <= threshold)
                continue;

            double listFeatureClassSU = listFeature.getClassSU();

            if ((listFeatureClassSU > featureClassMU)  && (betweenMutualInformation > featureClassMU)) {
                return featureList;
            }
            if ((featureClassMU > listFeatureClassSU) && (betweenMutualInformation > listFeatureClassSU)) {
                itr.remove();
            }
        }
        featureList.add(feature);
        return featureList;
    }

    public static List<Feature> addImportantFeatures(List<Feature> featureList, List<Feature> otherFeatureList, double doubleRows, double threshold) {
        for(Feature otherFeature : otherFeatureList){
            Feature.addImportantFeatures(featureList, otherFeature, doubleRows, threshold);
        }
        return featureList;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Feature)) {
            return false;
        }

        Feature c = (Feature) o;

        return index==c.index;
    }
}