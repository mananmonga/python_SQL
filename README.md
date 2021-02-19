# This repository contains solutions to the CS591 L1 assignments from Manan Monga

## Pre-requisites for running queries:

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [Ray](https://ray.io)

## Input Data

Queries of assignment 4 expect txt files with space separators'

The query structure for the recommendation and backward tracing queries can be seen in the file tests.py

The code is istrumented for operators Scan, Select, Join, Groupby, OrderBy
The top 2 time consuming operators for recommendation query are Join an GroupBy

The lineage methods for all of these operators is traced as well. 
The top 2 most time consuming operators for backward tracing are Join and GroupBy again, although Select takes time as well.


To deploy my code on the MOC I had to take the following steps:
1. Access the OpenStack Dashboard at https://kaizen.massopen.cloud/dashboard/project
2. Go to Key Pairs tab under Compute and import public key of local machine
3. Add SSH capabilities to the Security Group
4. To launch a VM I went to Images under Compute, chose the OS I wanted and it launched a menu from which I set details, source and the flavor. Had to select Boot Source as Image and Create New Volume as No to get image from existing flavor
5. Flavor to select the machine was set as m1.small
6. Added the Key- Pair fom ones selected.
7. Click on Launch Instance
8. Switch to Instances Tab and assign a floating IP to the instance
9. Have to make sure the pool is set to external, popup displays IP, I got 128.31.26.210
10. ssh into this Master Instance 
11. Create a key-pair
12. Now using this node we can create more nodes in the cluster.

