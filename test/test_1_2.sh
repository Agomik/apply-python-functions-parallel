#!/bin/sh

# test1: bash test_1_2.sh test/helloworld.yml 10000 64 32 16 8 4 2 1
# test2: bash test_1_2.sh test/hogwarts_legacy_reviews.csv 100 64 32 16 8 4 2 1

TESTFILE=$1
ITERATIONS=$2
NW_LIST=("${@:3}")
echo "Test file: $TESTFILE"
echo "Number of workers: ${NW_LIST[@]}"
echo "Iterations: $ITERATIONS"

echo "Sequential"
python3.12 test_sequential.py test_functions $TESTFILE $ITERATIONS tim_sort multiply_by_answer_to_life_universe_everything

for NW in "${NW_LIST[@]}"
    do
        echo "$NW workers"

        echo "Pthread"
        ./test_pthread test_functions.py $TESTFILE $NW $ITERATIONS tim_sort multiply_by_answer_to_life_universe_everything

        echo "FastFlow"
        ./test_ff test_functions.py $TESTFILE $NW $ITERATIONS tim_sort multiply_by_answer_to_life_universe_everything
    done
