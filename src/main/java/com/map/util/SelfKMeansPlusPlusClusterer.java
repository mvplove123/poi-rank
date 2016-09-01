//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.map.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.math3.exception.ConvergenceException;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.exception.NumberIsTooSmallException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.Clusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.util.MathUtils;

public class SelfKMeansPlusPlusClusterer<T extends Clusterable> extends Clusterer<T> {
    private final int k;
    private final int maxIterations;
    private final RandomGenerator random;
    private final SelfKMeansPlusPlusClusterer.EmptyClusterStrategy emptyStrategy;

    public SelfKMeansPlusPlusClusterer(int k) {
        this(k, -1);
    }

    public SelfKMeansPlusPlusClusterer(int k, int maxIterations) {
        this(k, maxIterations, new EuclideanDistance());
    }

    public SelfKMeansPlusPlusClusterer(int k, int maxIterations, DistanceMeasure measure) {
        this(k, maxIterations, measure, new JDKRandomGenerator());
    }

    public SelfKMeansPlusPlusClusterer(int k, int maxIterations, DistanceMeasure measure, RandomGenerator random) {
        this(k, maxIterations, measure, random, SelfKMeansPlusPlusClusterer.EmptyClusterStrategy.LARGEST_VARIANCE);
    }

    public SelfKMeansPlusPlusClusterer(int k, int maxIterations, DistanceMeasure measure, RandomGenerator random, SelfKMeansPlusPlusClusterer.EmptyClusterStrategy emptyStrategy) {
        super(measure);
        this.k = k;
        this.maxIterations = maxIterations;
        this.random = random;
        this.emptyStrategy = emptyStrategy;
    }

    public int getK() {
        return this.k;
    }

    public int getMaxIterations() {
        return this.maxIterations;
    }

    public RandomGenerator getRandomGenerator() {
        return this.random;
    }

    public SelfKMeansPlusPlusClusterer.EmptyClusterStrategy getEmptyClusterStrategy() {
        return this.emptyStrategy;
    }

    public List<CentroidCluster<T>> chooseInitialCenters(Collection<T> points) {
        List pointList = Collections.unmodifiableList(new ArrayList(points));
        int numPoints = pointList.size();
        boolean[] taken = new boolean[numPoints];
        ArrayList resultSet = new ArrayList();
        int firstPointIndex = this.random.nextInt(numPoints);
        Clusterable firstPoint = (Clusterable)pointList.get(firstPointIndex);
        resultSet.add(new CentroidCluster(firstPoint));
        taken[firstPointIndex] = true;
        double[] minDistSquared = new double[numPoints];

        for(int distSqSum = 0; distSqSum < numPoints; ++distSqSum) {
            if(distSqSum != firstPointIndex) {
                double d = this.distance(firstPoint, (Clusterable)pointList.get(distSqSum));
                minDistSquared[distSqSum] = d * d;
            }
        }

        while(resultSet.size() < this.k) {
            double var22 = 0.0D;

            for(int r = 0; r < numPoints; ++r) {
                if(!taken[r]) {
                    var22 += minDistSquared[r];
                }
            }

            double var23 = this.random.nextDouble() * var22;
            int nextPointIndex = -1;
            double sum = 0.0D;

            int p;
            for(p = 0; p < numPoints; ++p) {
                if(!taken[p]) {
                    sum += minDistSquared[p];
                    if(sum >= var23) {
                        nextPointIndex = p;
                        break;
                    }
                }
            }

            if(nextPointIndex == -1) {
                for(p = numPoints - 1; p >= 0; --p) {
                    if(!taken[p]) {
                        nextPointIndex = p;
                        break;
                    }
                }
            }

            if(nextPointIndex < 0) {
                break;
            }

            Clusterable var24 = (Clusterable)pointList.get(nextPointIndex);
            resultSet.add(new CentroidCluster(var24));
            taken[nextPointIndex] = true;
            if(resultSet.size() < this.k) {
                for(int j = 0; j < numPoints; ++j) {
                    if(!taken[j]) {
                        double d1 = this.distance(var24, (Clusterable)pointList.get(j));
                        double d2 = d1 * d1;
                        if(d2 < minDistSquared[j]) {
                            minDistSquared[j] = d2;
                        }
                    }
                }
            }
        }

        return resultSet;
    }

    private int getNearestCluster(Collection<CentroidCluster<T>> clusters, T point) {
        double minDistance = 1.7976931348623157E308D;
        int clusterIndex = 0;
        int minCluster = 0;

        for(Iterator i$ = clusters.iterator(); i$.hasNext(); ++clusterIndex) {
            CentroidCluster c = (CentroidCluster)i$.next();
            double distance = this.distance(point, c.getCenter());
            if(distance < minDistance) {
                minDistance = distance;
                minCluster = clusterIndex;
            }
        }

        return minCluster;
    }

    private Clusterable centroidOf(Collection<T> points, int dimension) {
        double[] centroid = new double[dimension];
        Iterator i = points.iterator();

        while(i.hasNext()) {
            Clusterable p = (Clusterable)i.next();
            double[] point = p.getPoint();

            for(int i1 = 0; i1 < centroid.length; ++i1) {
                centroid[i1] += point[i1];
            }
        }

        for(int var8 = 0; var8 < centroid.length; ++var8) {
            centroid[var8] /= (double)points.size();
        }

        return new DoublePoint(centroid);
    }

    @Override
    public List<? extends Cluster<T>> cluster(Collection<T> collection) throws MathIllegalArgumentException, ConvergenceException {
        return null;
    }

    public static enum EmptyClusterStrategy {
        LARGEST_VARIANCE,
        LARGEST_POINTS_NUMBER,
        FARTHEST_POINT,
        ERROR;

        private EmptyClusterStrategy() {
        }
    }
}
