                    Big Data Analytics · Fall 2026
Activity 1: Estimating π with Spark RDDs


Learning objectives
•	Create an RDD with sc.parallelize and control its number of partitions.
•	Apply transformations (map, filter) and actions (count, reduce) to a distributed computation.
•	Observe lazy evaluation and how sample size and partitioning affect accuracy and run time.

Background: the Monte Carlo idea
Picture a unit circle (radius 1) inside a square with corners at (−1, −1) and (1, 1). The square has area 4 and the circle has area π. If we throw points uniformly at random into the square, the fraction that lands inside the circle approaches π / 4. So:
π ≈ 4 × (points inside the circle) / (total points)
A point (x, y) is inside the circle when x² + y² ≤ 1. Each point is independent, so the work is embarrassingly parallel and a natural fit for an RDD: every partition generates and tests its own points, and Spark combines the counts.
Starter code (spark-shell, Scala)
Complete the two TODOs. Run the code in spark-shell, where sc is already available.
val n      = 1000000      // number of random points
val slices = 4            // number of partitions
 
val points = sc.parallelize(1 to n, slices)
 
// TODO 1: map each element to a random point (x, y) in [-1, 1] x [-1, 1]
//         hint: math.random returns a Double in [0, 1)
val xy = points.map { _ => ??? }
 
// TODO 2: keep only the points inside the unit circle and count them
val inside = ???
 
val piEstimate = 4.0 * inside / n
println(f"n = $n%,d  pi ~ $piEstimate%.6f  error = ${math.abs(piEstimate - math.Pi)}%.6f")


Tasks:
1.	Implement. Complete the TODOs and print your estimate of π and its absolute error.
2.	Accuracy vs. sample size. Run with n = 10³, 10⁵, 10⁷ and 10⁸ (keep slices = 4). Record your results in the table below. Time each run, for example with spark.time {...}.
3.	Effect of partitions. Fix n = 10⁷ and run with slices = 1, 2, 4 and 8. Use xy.getNumPartitions to confirm the setting, and note how the run time changes. Look at the Spark UI (http://localhost:4040) to see the tasks per stage.
4.	Rewrite with reduce. Replace filter + count with a single map that returns 1 or 0, followed by reduce(_ + _). Confirm you get the same kind of estimate.

**********************************************************************************

Results table

n (samples)	           Estimated π	        |π̂ − π|	     Partitions        	Time (s)
10^3	3.168000	0.026407	4	0.9279
10^5	3.152120	0.010527	4	0.9106
10^7	3.141678	0.000086	4	4.9754
10^8	3.141590	0.000003	4	45.7331

Task: Effect of partitions

n (samples)	          Estimated π	                   Partitions            	Time (s)
10^7	3.141331	1	1.1471
10^7	3.142074	2	2.2169
10^7	3.141564	4	5.0333
10^7	3.142036	8	7.6217

Task : Rewrite with reduce


n (samples)	           Estimated π	                   Partitions         	Time (s)
10^7	3.141370	1	1.3378
10^7	3.141857	2	1.7691
10^7	3.141004	4	4.0715
10^7	3.142117	8	5.2535


n (samples)	            Estimated π        	|π̂ − π|   	Partitions	         Time (s)
10^3	3.168000	0.026407	4	1.3250
10^5	3.140560	0.001033	4	1.4887
10^7	3.141903	0.000311	4	4.8503
10^8	3.141502	0.000090	4	31.7753




