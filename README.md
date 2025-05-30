# ApplyPythonFunctions

![Parallel architecture of the implementation based on POSIX threads.](pthread_parallel_architecture.png)

C++ library that enables to apply Python functions in parallel threads.

The library header is in file `ApplyPythonFunctions.h`.

`ApplyPythonFunctions_pthread.cpp` is a POSIX thread implementation of the library.

`ApplyPythonFunctions_ff.cpp` implements the libray exploiting the FastFlow framework using the dedicated farm implemented in `ff_PythonFarm.cpp`.

The types shared among the implementations are defined in `APF_Types.h`.

~~A crash issue has been spotted in subinterpreters when importing the some modules in multiple subinterpreters. The bug report can be found [here](https://github.com/python/cpython/issues/116524).~~ Solved in Python 3.13
