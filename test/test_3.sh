#!/bin/sh

# bash test_3.sh test/helloworld.yml 100 64 32 16 8 4 2 1

TESTFILE=$1
ITERATIONS=$2
NW_LIST=("${@:3}")
echo "Test file: $TESTFILE"
echo "Number of workers: ${NW_LIST[@]}"
echo "Iterations: $ITERATIONS"
FUNCTIONS="tim_sort multiply_by_answer_to_life_universe_everything"
echo $FUNCTIONS

echo "Sequential"
python3.12 test_sequential.py test_functions_cpubound $TESTFILE $ITERATIONS $FUNCTIONS

for NW in "${NW_LIST[@]}"
    do
        echo "$NW workers"

        echo "Pthread"
        ./test_pthread test_functions_cpubound.py $TESTFILE $NW $ITERATIONS $FUNCTIONS

        echo "FastFlow"
        ./test_ff test_functions_cpubound.py $TESTFILE $NW $ITERATIONS $FUNCTIONS
    done
