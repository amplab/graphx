---
layout: global
title: Machine Learning Library (MLlib)
---

MLlib is a Spark implementation of some common machine learning (ML)
functionality, as well associated tests and data generators.  MLlib
currently supports four common types of machine learning problem settings,
namely, binary classification, regression, clustering and collaborative
filtering, as well as an underlying gradient descent optimization primitive.
This guide will outline the functionality supported in MLlib and also provides
an example of invoking MLlib.

# Dependencies
MLlib uses the [jblas](https://github.com/mikiobraun/jblas) linear algebra library, which itself
depends on native Fortran routines. You may need to install the 
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes. MLlib will throw a linking error if it cannot 
detect these libraries automatically.

# Binary Classification

Binary classification is a supervised learning problem in which we want to
classify entities into one of two distinct categories or labels, e.g.,
predicting whether or not emails are spam.  This problem involves executing a
learning *Algorithm* on a set of *labeled* examples, i.e., a set of entities
represented via (numerical) features along with underlying category labels.
The algorithm returns a trained *Model* that can predict the label for new
entities for which the underlying label is unknown. 
 
MLlib currently supports two standard model families for binary classification,
namely [Linear Support Vector Machines
(SVMs)](http://en.wikipedia.org/wiki/Support_vector_machine) and [Logistic
Regression](http://en.wikipedia.org/wiki/Logistic_regression), along with [L1
and L2 regularized](http://en.wikipedia.org/wiki/Regularization_(mathematics))
variants of each model family.  The training algorithms all leverage an
underlying gradient descent primitive (described
[below](#gradient-descent-primitive)), and take as input a regularization
parameter (*regParam*) along with various parameters associated with gradient
descent (*stepSize*, *numIterations*, *miniBatchFraction*). 

The following code snippet illustrates how to load a sample dataset, execute a
training algorithm on this training data using a static method in the algorithm
object, and make predictions with the resulting model to compute the training
error.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

// Load and parse the data file
val data = sc.textFile("mllib/data/sample_svm_data.txt")
val parsedData = data.map { line =>
  val parts = line.split(' ')
  LabeledPoint(parts(0).toDouble, parts.tail.map(x => x.toDouble).toArray)
}

// Run training algorithm
val numIterations = 20
val model = SVMWithSGD.train(parsedData, numIterations)
 
// Evaluate model on training examples and compute training error
val labelAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
println("trainError = " + trainErr)
{% endhighlight %}

The `SVMWithSGD.train()` method by default performs L2 regularization with the
regularization parameter set to 1.0. If we want to configure this algorithm, we
can customize `SVMWithSGD` further by creating a new object directly and
calling setter methods. All other MLlib algorithms support customization in
this way as well. For example, the following code produces an L1 regularized
variant of SVMs with regularization parameter set to 0.1, and runs the training
algorithm for 200 iterations. 

{% highlight scala %}
import org.apache.spark.mllib.optimization.L1Updater

val svmAlg = new SVMWithSGD()
svmAlg.optimizer.setNumIterations(200)
  .setRegParam(0.1)
  .setUpdater(new L1Updater)
val modelL1 = svmAlg.run(parsedData)
{% endhighlight %}

Both of the code snippets above can be executed in `spark-shell` to generate a
classifier for the provided dataset.

Available algorithms for binary classification:

* [SVMWithSGD](api/mllib/index.html#org.apache.spark.mllib.classification.SVMWithSGD)
* [LogisticRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD)

# Linear Regression

Linear regression is another classical supervised learning setting.  In this
problem, each entity is associated with a real-valued label (as opposed to a
binary label as in binary classification), and we want to predict labels as
closely as possible given numerical features representing entities.  MLlib
supports linear regression as well as L1
([lasso](http://en.wikipedia.org/wiki/Lasso_(statistics)#Lasso_method)) and L2
([ridge](http://en.wikipedia.org/wiki/Ridge_regression)) regularized variants.
The regression algorithms in MLlib also leverage the underlying gradient
descent primitive (described [below](#gradient-descent-primitive)), and have
the same parameters as the binary classification algorithms described above. 

Available algorithms for linear regression: 

* [LinearRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.regression.LinearRegressionWithSGD)
* [RidgeRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.regression.RidgeRegressionWithSGD)
* [LassoWithSGD](api/mllib/index.html#org.apache.spark.mllib.regression.LassoWithSGD)

# Clustering

Clustering is an unsupervised learning problem whereby we aim to group subsets
of entities with one another based on some notion of similarity.  Clustering is
often used for exploratory analysis and/or as a component of a hierarchical
supervised learning pipeline (in which distinct classifiers or regression
models are trained for each cluster). MLlib supports
[k-means](http://en.wikipedia.org/wiki/K-means_clustering) clustering, arguably
the most commonly used clustering approach that clusters the data points into
*k* clusters. The MLlib implementation includes a parallelized 
variant of the [k-means++](http://en.wikipedia.org/wiki/K-means%2B%2B) method
called [kmeans||](http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf).
The implementation in MLlib has the following parameters:  

* *k* is the number of clusters.
* *maxIterations* is the maximum number of iterations to run.
* *initializationMode* specifies either random initialization or
initialization via k-means\|\|.
* *runs* is the number of times to run the k-means algorithm (k-means is not
guaranteed to find a globally optimal solution, and when run multiple times on
a given dataset, the algorithm returns the best clustering result).
* *initializiationSteps* determines the number of steps in the k-means\|\| algorithm.
* *epsilon* determines the distance threshold within which we consider k-means to have converged. 

Available algorithms for clustering: 

* [KMeans](api/mllib/index.html#org.apache.spark.mllib.clustering.KMeans)

# Collaborative Filtering 

[Collaborative filtering](http://en.wikipedia.org/wiki/Recommender_system#Collaborative_filtering)
is commonly used for recommender systems.  These techniques aim to fill in the
missing entries of a user-item association matrix.  MLlib currently supports
model-based collaborative filtering, in which users and products are described
by a small set of latent factors that can be used to predict missing entries.
In particular, we implement the [alternating least squares
(ALS)](http://www2.research.att.com/~volinsky/papers/ieeecomputer.pdf)
algorithm to learn these latent factors. The implementation in MLlib has the
following parameters:

* *numBlocks* is the number of blacks used to parallelize computation (set to -1 to auto-configure). 
* *rank* is the number of latent factors in our model.
* *iterations* is the number of iterations to run.
* *lambda* specifies the regularization parameter in ALS.
* *implicitPrefs* specifies whether to use the *explicit feedback* ALS variant or one adapted for *implicit feedback* data
* *alpha* is a parameter applicable to the implicit feedback variant of ALS that governs the *baseline* confidence in preference observations

## Explicit vs Implicit Feedback

The standard approach to matrix factorization based collaborative filtering treats 
the entries in the user-item matrix as *explicit* preferences given by the user to the item.

It is common in many real-world use cases to only have access to *implicit feedback* 
(e.g. views, clicks, purchases, likes, shares etc.). The approach used in MLlib to deal with 
such data is taken from 
[Collaborative Filtering for Implicit Feedback Datasets](http://research.yahoo.com/pub/2433).
Essentially instead of trying to model the matrix of ratings directly, this approach treats the data as 
a combination of binary preferences and *confidence values*. The ratings are then related 
to the level of confidence in observed user preferences, rather than explicit ratings given to items. 
The model then tries to find latent factors that can be used to predict the expected preference of a user
for an item. 

Available algorithms for collaborative filtering: 

* [ALS](api/mllib/index.html#org.apache.spark.mllib.recommendation.ALS)

# Gradient Descent Primitive

[Gradient descent](http://en.wikipedia.org/wiki/Gradient_descent) (along with
stochastic variants thereof) are first-order optimization methods that are
well-suited for large-scale and distributed computation. Gradient descent
methods aim to find a local minimum of a function by iteratively taking steps
in the direction of the negative gradient of the function at the current point,
i.e., the current parameter value. Gradient descent is included as a low-level
primitive in MLlib, upon which various ML algorithms are developed, and has the
following parameters:

* *gradient* is a class that computes the stochastic gradient of the function
being optimized, i.e., with respect to a single training example, at the
current parameter value. MLlib includes gradient classes for common loss
functions, e.g., hinge, logistic, least-squares.  The gradient class takes as
input a training example, its label, and the current parameter value. 
* *updater* is a class that updates weights in each iteration of gradient
descent. MLlib includes updaters for cases without regularization, as well as
L1 and L2 regularizers.
* *stepSize* is a scalar value denoting the initial step size for gradient
descent. All updaters in MLlib use a step size at the t-th step equal to
stepSize / sqrt(t). 
* *numIterations* is the number of iterations to run.
* *regParam* is the regularization parameter when using L1 or L2 regularization.
* *miniBatchFraction* is the fraction of the data used to compute the gradient
at each iteration.

Available algorithms for gradient descent:

* [GradientDescent](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent)
