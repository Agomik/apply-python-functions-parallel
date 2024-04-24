#!/bin/sh

echo "Compiling pthread implementation..."
g++ -I/usr/include/python3.12 -std=c++17 -O3 -o test_pthread test_parallel.cpp ApplyPythonFunctions_pthread.cpp -pthread -lpython3.12
echo "Done."

echo "Compiling FF implementation..."
g++ -I/usr/include/python3.12 -I./fastflow -std=c++17 -O3 -o test_ff test_parallel.cpp ApplyPythonFunctions_ff.cpp -lpython3.12
echo "Done."