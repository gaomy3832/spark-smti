# SMTI
Stable Marriage with Incomplete List and Ties (SMTI) using Spark

Course project by Yilong Geng and Mingyu Gao for CME 323 in Stanford
University.

Algorithm
---------
Implement Gale-Shapley algorithm for stable marriage problem, and Zoltan
Kiraly's algorithm for 3/2-approximation on SMTI problem.

Source File Structure
---------------------
- src/main/scala/edu/stanford/cme323/spark/smti/
    - package.scala: general definition
    - SMTIGS.scala: base class for GS algorithm scheme
    - SMTIGSBasic.scala: basic GS algorithm
    - SMTIGSKiraly.scala: Kiraly's algorithm
    - utils/: utilities for IO and random data generation
- MainApp.scala: example usage main class
- utilities/
    - transInputData.py: script to translate dataset at http://cri-hpc1.univ-paris1.fr/smti/

Quick Start
-----------
**Compile**

Use sbt:
`make assembly`

**Run**

Use random generated data:
`make run ARGS="--size 1000 --pi 0.9 --pt 0.2"`

Use translated dataset:
`make run ARGS="--in <data dir>"`

Use default random generated data, and output marriage result:
`make run ARGS="--out <out dir>"`

Specify a seed for random generator:
`make run ARGS="--seed <seed>"`

Specify number of RDD partitions:
`make run ARGS="--num-parts <np>"`

Specify number of working threads:
`make run N=<n>`

References
----------
- Gale, D., Shapley, L. S.. College Admissions and the Stability of Marriage.
American Mathematical Monthly.
- Kiraly, Z.. Linear Time Local Approximation Algorithm for Maximum Stable
Marriage.  Algorithms 2013.

