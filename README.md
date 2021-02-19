# This repository contains a library with popular SQL operators implemented in Python to test efficiency and use of data provenance features, ML explainability libraries and also performance metric dashboards

Topics
Resources


## Pre-requisites for running queries:

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [Ray](https://ray.io)

## Input Data
The task queries expect two space-delimited text files (similar to CSV files). 

The first file (friends) must include records of the form:

|UID1 (int)|UID2 (int)|
|----|----|
|1   |2342|
|231 |3   |
|... |... |

The second file (ratings) must include records of the form:

|UID (int)|MID (int)|RATING (int)|
|---|---|------|
|1  |10 |4     |
|231|54 |2     |
|...|...|...   |

## Running queries 

You can run queries as shown below: 

```bash
$ python engine.py --task [task_number] --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id]
```

For example, the following command runs the 'likeness prediction' query of the first task for user id 1 and movie id 1:

```bash
$ python3 skeleton/engine.py task1 --friends 'data/friends.txt' --ratings 'data/movie_ratings.txt' --uid '1' --mid '1'
```

The 'recommendation' query of the second task does not require a movie id. Provide a limit instead.
```bash
$ python3 skeleton/assignment_12.py task2 --friends 'data/friends.txt' --ratings 'data/movie_ratings.txt' --uid '1' --limit '1'
```
$ python3 skeleton/engine.py task3 --friends 'data/friends.txt' --ratings 'data/movie_ratings.txt' --uid '1' --mid '1'


## Running queries of Assignment 2

TODO
