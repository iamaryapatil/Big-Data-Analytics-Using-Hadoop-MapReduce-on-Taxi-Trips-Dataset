# Big-Data-Analytics-Using-Hadoop-MapReduce-on-Taxi-Trips-Dataset

This project demonstrates an end-to-end distributed big data processing pipeline built using Hadoop MapReduce and Python (Hadoop Streaming). The goal is to analyze large-scale taxi trip data to extract operational and business insights, including trip categorization, clustering geographic trip endpoints, and ranking taxi companies by total trip volume.

#### Project Overview

The pipeline consists of three independent tasks, executed using Hadoop Streaming jobs:

#### Task 1 — Trip Distance Categorization
Classified taxi trips into distance buckets using a MapReduce job.
- Computed trip distance using pickup/dropoff coordinates.
- Used in-mapper combining to reduce shuffle overhead.
- Parallel processing with 3 reducer tasks for distributed aggregation.

#### Task 2 — K-Medoids Clustering
Clustered trip locations to find high-demand geographic zones.
- Implemented iterative assign → update structure using chained Hadoop jobs.
- Recomputed medoids each iteration until convergence or max iterations.
- Performed geospatial clustering based on pickup/dropoff points.

#### Task 3 — Taxi Company Ranking
Built a multi-stage Hadoop pipeline to join datasets and compute total trips per taxi company.
- Job 1: Join Trips.txt & Taxis.txt using Reducer-side join
- Job 2: Aggregate trip counts per company
- Job 3: Distributed sorting with custom partitioning + comparators

#### Key Features
- End-to-end distributed computation pipeline
- Iterative clustering process controlled via Bash shell script
- Optimized shuffle & reducer performance using combiners
- Custom distributed sorting logic using bucket-based key partitioning
- Clean modular architecture for scalability

#### How to Run
#### Upload datasets to HDFS
hadoop fs -put Taxis.txt /Input/

hadoop fs -put Trips.txt /Input/

#### Run each task
Task1-run.sh

Task2-run.sh

Task3-run.sh
