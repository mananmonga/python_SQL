from __future__ import absolute_import, annotations, division, print_function

import argparse
import csv
import logging
import sys
import time
import uuid
from collections import defaultdict
from typing import List, Tuple

import ray

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()


# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage() -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how() -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs() -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    def __init__(self, id=None, name=None, track_prov=False,
                                           propagate_prov=False):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self):
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

# Scan operator
@ray.remote
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes scan operator
    def __init__(self, filepath, filter=None, track_prov=False,
                                              propagate_prov=False):
        super().__init__(name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.filepath = filepath
            

    # Returns next batch of tuples in given file (or None if file exhausted)
    
    def get_next(self):
        # YOUR CODE HERE
        filepath = self.filepath
        rows= []
        
        with open(filepath) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=' ')
            for row in csv_reader:
                    rows.append(row)
            return rows
            

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Equi-join operator
@ray.remote
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_input (Operator): A handle to the left input.
        right_input (Operator): A handle to the left input.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes join operator
    def __init__(self, left_input, right_input, left_join_attribute,
                                                right_join_attribute,
                                                track_prov=False,
                                                propagate_prov=False):
        super().__init__(name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.left_input = left_input
        self.right_input = right_input
        self.right_join_attribute = right_join_attribute
        self.left_join_attribute = left_join_attribute

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        left_input = self.left_input
        right_input = self.right_input
        right_join_attribute = self.right_join_attribute
        left_join_attribute = self.left_join_attribute
        rows = []
        leftList = ray.get(left_input.get_next.remote())
        rightList = ray.get(right_input.get_next.remote())
        for rowL in leftList:
            for rowR in rightList:
                if(rowL[left_join_attribute]==rowR[right_join_attribute]):
                    row = rowL+rowR
                    rows.append(row)
        return(rows)

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Project operator
@ray.remote
class Project(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes project operator
    def __init__(self, input, fields_to_keep=[], track_prov=False,
                                                 propagate_prov=False):
        super().__init__(name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.fields_to_keep = fields_to_keep
                    

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        input = self.input
        fields = self.fields_to_keep
        inputList = ray.get(input.get_next.remote())
        rows= []
        for row in inputList:
            current = []
            for index in fields:
                current.append(row[index])
            rows.append(current) 
        #print(rows)
        return rows

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Group-by operator
@ray.remote
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes groupby operator
    def __init__(self, input, key, value, agg_fun, track_prov=False,
                                                   propagate_prov=False):
        super().__init__(name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.key = key
        self.value = value
        self.agg_fun = agg_fun


    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        input = self.input
        key = self.key
        value = self.value
        agg_fun = self.agg_fun
        inputList = ray.get(input.get_next.remote())
        movie_rating_sum = defaultdict(float)
        movie_rating_count = defaultdict(int)
        for index in range(len(inputList)):
            uid,mid,rating = inputList[index]
            movie_rating_sum[mid] += float(rating)
            movie_rating_count[mid] += 1

        friendsAvgRating = []
        for mid in movie_rating_count:
            friends_rating_row = [mid , agg_fun(movie_rating_sum[mid],movie_rating_count[mid])]
            friendsAvgRating.append(friends_rating_row) 
        return friendsAvgRating




    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Custom histogram operator
@ray.remote
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes histogram operator
    def __init__(self, input, key=0, track_prov=False, propagate_prov=False):
        super().__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.key = key

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        key = self.key
        input = self.input
        hist_dict = defaultdict(int)
        allowed_values = {'0','1','2','3','4','5'}
        inputList = ray.get(input.get_next.remote())
        for row in inputList:
            if row[2] in allowed_values:
                hist_dict[row[2]] += 1
        return hist_dict
# Order by operator
@ray.remote
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        input (Operator): A handle to the input
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes order-by operator
    def __init__(self, input, comparator, ASC=True, track_prov=False,
                                                    propagate_prov=False):
        super().__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.comparator = comparator
        self.ASC = ASC

    # Returns the sorted input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        input = self.input
        comparator = self.comparator
        ASC = self.ASC
        inputList = ray.get(input.get_next.remote())
        orderedList = comparator(inputList, ASC)
        return orderedList


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Top-k operator
class TopK(Operator):
    """TopK operator.

    Attributes:
        input (Operator): A handle to the input.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes top-k operator
    def __init__(self, input, k=None, track_prov=False, propagate_prov=False):
        super(TopK, self).__init__(name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Select operator
@ray.remote
class Select(Operator):
    """Select operator.

    Attributes:
        input (Operator): A handle to the input.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes select operator
    def __init__(self, input, predicate, track_prov=False,
                                         propagate_prov=False):
        super().__init__(name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.predicate = predicate


    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        predicate = self.predicate
        input = self.input
        inputList = ray.get(input.get_next.remote())
        rows = []
        for row in inputList:
            if predicate(row) == True:
                rows.append(row)
        return rows

def Task1(friendsFile , ratingsFile , uid , mid):
    
    def isFriend(input):
        if input[0]==uid:
            return True
    def selectMov(input):
        if input[1]==mid:
            return True
    def AVG(input):
        count = 0
        sum = 0
        for nums in input:
            for num in nums:
                num = int(num)
                sum = sum + num
                count = count+1
            
        avg = sum/count
        return avg
           
    friends = Scan.remote(friendsFile) #create table 1 operator
    movie_ratings = Scan.remote(ratingsFile) #create table 2 operator
    friendList = Select.remote(friends,isFriend) #filter friends of given uid
    friendListCol = Project.remote(friendList,[1]) #reduce list of friends
    currMovie = Select.remote(movie_ratings,selectMov) #ratings of given mid
    ratingList = Join.remote(friendListCol,currMovie,0,0) #join to get ratings of friends for given mid
    likeness = Project.remote(ratingList,[3]) #get list of ratings
    likenessList = likeness.get_next.remote() 
    likenessRating = AVG(ray.get(likenessList)) #do average on the ratings
    return(likenessRating)

def Task2(friendsFile,ratingsFile,uid,limit):
    def isFriend(input):
        if input[0]==uid:
            return True
    def AVG(sum,count):
        avg = sum/count
        return avg
    def ratingSort(inputList, ASC):
        if(ASC):
            lis = sorted(inputList, key = lambda x: x[1])
        else:
            lis = sorted(inputList, key = lambda x: x[1], reverse=True)
        return lis

    def isFriends(input):
        
        for value in friendsList:
            if input[0] == value[0]:
                return True
            
    limit = 1       
    filef = friendsFile
    friends = Scan.remote(filef)
    filename = ratingsFile
    movie_ratings = Scan.remote(filename)
    uid = uid
    limit = limit
    friendsList = Select.remote(friends, isFriend)#list of friends with given uid
    friendsList_1 = Project.remote(friendsList,[1])#list of friends
    friendsList =ray.get(friendsList_1.get_next.remote())#for isFriends
    ratingsList = Select.remote(movie_ratings,isFriends)
    averageRatings = GroupBy.remote(ratingsList,1,2,AVG)
    recommendation = OrderBy.remote(averageRatings,ratingSort, False)
    recommendation =ray.get(recommendation.get_next.remote())
    return(recommendation[:limit])

def Task3(friendsFile,moviesFile,uid,mid):
    def selectMov(input):
        if (input[1])==mid:
            return True
    def isFriend(input):
        
        if (input[0])==uid:
            return True
    def isFriends(input):
        for value in friendsList:
            if input[0] == value[0]:
                return True
    mid = mid
    uid = uid
    filef = friendsFile
    friends = Scan.remote(filef)
    filename = moviesFile
    movieRatings = Scan.remote(filename)
    currMovie = Select.remote(movieRatings,selectMov)
    friendsList = Select.remote(friends, isFriend)#list of friends with given uid
    friendsList = Project.remote(friendsList,[1])#list of friends
    friendsList =ray.get(friendsList.get_next.remote())#for isFriends
    ratingsList = Select.remote(currMovie,isFriends)
    outList = Histogram.remote(ratingsList,2)
    dic = ray.get(outList.get_next.remote())
    sorted_keys = sorted(dic.keys())
    out= []
    for key in sorted_keys:
        out.append('{}:{}'.format(key, dic[key]))
    return out
    
    
if __name__ == "__main__":
    initial_time = time.time()
    ray.init(local_mode=True)
    logger.info("Assignment #1")
    #print(Task1('data/friends.txt','data/movie_ratings.txt','1','1'))

    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'
    
    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    # YOUR CODE HERE
    parser = argparse.ArgumentParser(
        description='Inventory DB CLI')
    subparser = parser.add_subparsers(dest='task')
    # task 1 subparser
    task1_parser = subparser.add_parser(
        'task1', help='task1')
    task1_parser.add_argument('--friends', type=str, required=True)
    task1_parser.add_argument('--ratings', type=str, required=True)
    task1_parser.add_argument('--uid', type=str, required=True)
    task1_parser.add_argument('--mid', type=str, required=True)
    # task 2 subparser
    task2_parser = subparser.add_parser(
        'task2', help='task2')
    task2_parser.add_argument('--friends', type=str, required=True)
    task2_parser.add_argument('--ratings', type=str, required=True)
    task2_parser.add_argument('--uid', type=str, required=True)
    task2_parser.add_argument('--limit', type=str, required=True)
    # task 3 subparser
    task3_parser = subparser.add_parser(
        'task3', help='task3')
    task3_parser.add_argument('--friends', type=str, required=True)
    task3_parser.add_argument('--ratings', type=str, required=True)
    task3_parser.add_argument('--uid', type=str, required=True)
    task3_parser.add_argument('--mid', type=str, required=True)
    
    cli_command = parser.parse_args()

    task = vars(cli_command)['task']
    if task == 'task1':
        friends = getattr(cli_command, 'friends')
        ratings = getattr(cli_command, 'ratings')
        uid = getattr(cli_command, 'uid')
        mid  = getattr(cli_command, 'mid')
        print(Task1(friends,ratings,uid,mid))
        
    elif task == 'task2':
        friends = getattr(cli_command, 'friends')
        ratings = getattr(cli_command, 'ratings')
        uid = getattr(cli_command, 'uid')
        limit  = getattr(cli_command, 'limit')
        print(Task2(friends,ratings,uid,limit))


    elif task == 'task3':
        friends = getattr(cli_command, 'friends')
        ratings = getattr(cli_command, 'ratings')
        uid = getattr(cli_command, 'uid')
        mid  = getattr(cli_command, 'mid')
        print(Task3(friends,ratings,uid,mid))
    else:
        logging.error('Invalid args.')



    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    
    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
    