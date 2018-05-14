# Databricks notebook source
# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import json


# ------------------------------------------
# Parse to tuple of cuisine and reviews
# ------------------------------------------
def parse_tuple(x):
    cuisine = x["cuisine"]
    points = x["points"]
    evaluation = x["evaluation"]
    return cuisine, (points, evaluation)


# ------------------------------------------
# Parse to tuple of cuisine and number of
# reviews, number of negative reviews, total 
# points and quality
# ------------------------------------------
def parse_reviews_point(x):
    n_review = 0
    n_neg_review = 0
    pts = 0
    print(x)
    for each in x[1]:
        rev = each[1]
        n_review += 1
        if rev[1] == "Negative":
            n_neg_review += 1
            pts -= rev[0]
            continue
        else:
            pts += rev[0]
    quality = pts / n_review
    return x[0], (n_review, n_neg_review, pts, quality)


# ------------------------------------------
# Remove cuisines with fewer reviews than 
# average number of reviews and negative 
# percentage larger than given percent
# ------------------------------------------
def remove_vals(x, avg, percent):
    n_review = x[1][0]
    n_neg_review = x[1][1]
    if n_review <= avg or ((n_neg_review / n_review) * 100) > percent:
        return False
    else:
        return True


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
    # Part 1
    input_rdd = sc.textFile(dataset_dir)
    map_rdd = input_rdd.map(lambda x: json.loads(x))
    parsed_rdd = map_rdd.map(parse_tuple)
    group_rdd = parsed_rdd.groupBy(lambda x: x[0])
    formatted_rdd = group_rdd.map(parse_reviews_point)
    # Part 2
    avg_cuisine_reviews = map_rdd.count() / formatted_rdd.count()
    # Part 3
    vals_removed_rdd = formatted_rdd.filter(lambda x: remove_vals(x, avg_cuisine_reviews, percentage_f))
    # Part 4
    final_rdd = vals_removed_rdd.sortBy(lambda x: x[1][3], ascending=False)
    final_rdd.saveAsTextFile(result_dir)
    print(final_rdd.collect())


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02_my_databricks/"
    result_dir = "/FileStore/tables/A02_my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)


# COMMAND ----------


