#include <iostream>
#include <thread>
#include <deque>
#include <future>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <memory>
#include <filesystem>
#include <optional>
#include <Python.h>
#include "ff_PythonFarm.hpp"
#include "ff/ff.hpp"

using namespace ff;

// Inizialization flag to avoid repeated APF_Initialize
bool initialized = false;

// The actual Python Farm
ff_PythonFarm* f;
ff_PythonFarm_async_emitter* e;
ff_PythonFarm_collector* c;

int APF_Initialize(
    bool initialize_python,
    const std::string &python_module_name,
    unsigned int n_workers
) {
    #ifdef DEBUG
    std::cerr << "APF called" << std::endl;
    #endif

    if(!initialized) {
        // Initialize Python if requested
        if(initialize_python) {
            Py_Initialize();
        }
        unsigned int numWorkers = n_workers;
        #ifdef DEBUG
        std::cerr << "Main thread state is " << PyGILState_GetThisThreadState() << ", interpreter is " << PyThreadState_GetInterpreter(PyGILState_GetThisThreadState()) << std::endl;
        #endif

        // Initialize the farm and run it
        e = new ff_PythonFarm_async_emitter();
        c = new ff_PythonFarm_collector();
        f = new ff_PythonFarm(numWorkers, python_module_name);
        f->add_emitter(e);
        f->add_collector(c);
        f->set_scheduling_ondemand(1);
        f->run();

        // Reset call count for task chunk IDs and set initialization flag
        initialized = true;
        #ifdef DEBUG
        std::cerr << "Initialization ended" << std::endl;
        #endif

        return 0;
    } else {
        return -1;
    }
}

void APF_Finalize(bool finalize_python) {
    #ifdef DEBUG
    std::cerr << "APF_Finalize called. Sleeping two seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cerr << "APF_Finalize slept." << std::endl;
    #endif
    // Add EOS to the shared queue
    e->send_eos();
    #ifdef DEBUG
    std::cerr << "APF_Finalize EOS sent. Sleeping two seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cerr << "APF_Finalize slept." << std::endl;
    std::cerr << "APF_Finalize is now waiting f's collector..." << std::endl;
    #endif
    f->wait_collector();
    #ifdef DEBUG
    std::cerr << "APF_Finalize has ended Collector wait. Deleting..." << std::endl;
    #endif
    #ifdef DEBUG
    std::cerr << "Deleting f" << std::endl;
    #endif
    delete f;
    #ifdef DEBUG
    std::cerr << "Deleting e" << std::endl;
    #endif
    delete e;
    #ifdef DEBUG
    std::cerr << "Deleting c" << std::endl;
    #endif
    delete c;

    initialized = false;
    #ifdef DEBUG
    std::cerr << "Finalization ended" << std::endl;
    #endif

    // Reacquire the GIL and finalize Python
    /*if(finalize_python) {
        auto finalization_code = Py_FinalizeEx();
        if(finalization_code < 0) {
            std::cerr << "Error finalizing Python" << std::endl;
        } else {
            std::cerr << "Python finalized correctly" << std::endl;
        }
    }*/
}

std::vector<std::shared_future<PythonParallelResult>> APF_ParallelApply(
    const char* data,
    const std::vector<std::string>& function_names
) {
    return e->apply(data, function_names);
}