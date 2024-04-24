#include <iostream>
#include <filesystem>
#include <vector>
#include <fstream>
#include <future>
#include <thread>
#include <numeric>
#include <algorithm>
#include <Python.h>
#include "ApplyPythonFunctions.h"

int main(int argc, char *argv[]) {

    // Set the Python module name
    if(argc < 2) {
        std::cerr << "You need to specify a python module." << std::endl;
        return -1;
    }
    if(argc < 3) {
        std::cerr << "You need to specify a data file." << std::endl;
        return -1;
    }

    if(argc < 4) {
        std::cerr << "You need to specify the number of workers." << std::endl;
        return -1;
    }

    if(argc < 5) {
        std::cerr << "You need to specify the number of iterations." << std::endl;
        return -1;
    }

    if(argc < 6) {
        std::cerr << "You need to specify at least one function name." << std::endl;
        return -1;
    }

    // Start time measurement
    std::chrono::time_point computation_start = std::chrono::system_clock::now();

    // Perform the tests
    std::string python_module_name = argv[1];
    int nw = atoi(argv[3]);
    std::vector<PythonParallelResult> results;
    APF_Initialize(true, python_module_name, nw);
    int iterations = atoi(argv[4]);
    std::vector<char*> inputs;
    std::vector<std::shared_future<PythonParallelResult>> futures;
    std::chrono::time_point test_ready = std::chrono::system_clock::now();
    for(int i = 0; i < iterations; i++) {
        #ifdef DEBUG
        std::cerr << "Iteration #" << i+1 << "/" << iterations << std::endl;
        #endif
        
        // Read the text file from disk
        std::string datapath = std::filesystem::current_path().string() + "/" + argv[2];
        std::ifstream file(datapath, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error opening data file at " << datapath << std::endl;
            return -1;
        }

        std::vector<char> file_buffer(std::istreambuf_iterator<char>(file), {});
        std::string serialized(file_buffer.data(), file_buffer.size());
        file.close();

        // Set all the functions to be applied
        std::vector<std::string> fnames;
        for(int i = 5; i < argc; i++) {
            fnames.push_back(argv[i]);
        }

        // Apply the selected Python functions to data
        std::vector<std::shared_future<PythonParallelResult>> iteration_futures = APF_ParallelApply(serialized.data(), fnames);
        for(auto& future : iteration_futures) {
            futures.emplace_back(std::move(future));
        }
    }

    for (auto& result : futures) {
        auto value = result.get();
        results.emplace_back(value);
        #ifdef DEBUG
            std::cerr << "test_parallel received the result value" << std::endl;
        #endif
    }

    // Compute measurements
    std::chrono::time_point computation_end = std::chrono::system_clock::now();
    std::cout << "Completion time: " << std::chrono::duration_cast<std::chrono::microseconds>(computation_end - computation_start).count() << std::endl;
    std::cout << "Initialization overhead: " << std::chrono::duration_cast<std::chrono::microseconds>(test_ready - computation_start).count() << std::endl;
    std::cout << "Computation time: " << std::chrono::duration_cast<std::chrono::microseconds>(computation_end - test_ready).count() << std::endl;

    // Compute average service time
    std::vector<std::chrono::system_clock::time_point> result_times(iterations);
    for(auto& r : results) {
        #ifdef DEBUG
        std::cerr << "End time detected for iteration #" << r.id << std::endl;
        #endif
        if(r.time > result_times[r.id]) {
            #ifdef DEBUG
            std::cerr << "The new time detection is later than the previous for iteration #" << r.id << std::endl;
            #endif
            result_times[r.id] = r.time;
        } else {
            #ifdef DEBUG
            std::cerr << "No end time update for iteration #" << r.id << std::endl;
            #endif
        }
    }

    // Compute and print service times
    if(iterations > 1) {
        std::vector<long> service_times;
        for(int i = 1; i < iterations; i++) {
            std::sort(result_times.begin(), result_times.end());
            long duration = std::chrono::duration_cast<std::chrono::microseconds>(result_times[i] - result_times[i-1]).count();
            service_times.emplace_back(duration);
            #ifdef DEBUG
            std::cout << "Service time for iteration #" << i << ": " << duration << std::endl;
            #endif
        }
        long average = std::accumulate(service_times.begin(), service_times.end(), 0L) / service_times.size();
        std::cout << "Average service time: " << average << std::endl;
    }

    // Closing
    APF_Finalize(true);

    return 0;
}