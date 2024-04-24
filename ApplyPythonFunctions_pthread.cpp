#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <deque>
#include <future>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <memory>
#include <filesystem>
#include <cstring>
#include <optional>
#include <Python.h>
#include "ApplyPythonFunctions.h"

// Lazy initialization at first call
bool initialized = false;
char* python_module_name;
char* python_module_code;
unsigned short call_count;

// Task queue shared among threads
std::deque<std::optional<PythonParallelTask>> taskQueue;
std::mutex queueMutex;
std::condition_variable queueCondition;
PyThreadState *_save;

// Thread pool end barrier
std::mutex end_mutex;
std::condition_variable cv_end;
unsigned int active_threads;

void worker(int worker_id) {

    // Initialize the worker

    PyGILState_STATE gstate = PyGILState_Ensure();
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 0,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,
    };
    PyThreadState *tstate = NULL;
    // Create new subinterpreter (automatically releases the main interpreter's GIL and acquires the new subinterpreter's)
    PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);
    #ifdef DEBUG
    std::cerr << "Subinterpreter started in thread " << worker_id << std::endl;
    std::cerr << worker_id << "interpreter is: " << PyThreadState_GetInterpreter(tstate) << ", tstate is " << PyThreadState_Get() << std::endl;
    #endif
    if (PyStatus_Exception(status)) {
        std::cerr << worker_id << "Error creating interpreter in thread." << std::endl;
        PyGILState_Release(gstate);
        return;
    }
    // Adds the cwd to the (sub)interpreter's Python path for the subinterpreter
    auto cwd = std::filesystem::current_path().string();
    std::string sysPathCmd = "import sys\nsys.path.append('" + cwd + "')";
    PyRun_SimpleString(sysPathCmd.c_str());
    if (PyStatus_Exception(status)) {
        std::cerr << "Error creating interpreter in thread." << std::endl;
        PyGILState_Release(gstate);
        return;
    } else {
        #ifdef DEBUG
        std::cerr << "Cwd added to path for loading modules." << std::endl;
        #endif
    }
    // Load the Python module
    PyObject* pModuleName = PyUnicode_FromString(python_module_name);
    #ifdef DEBUG
    std::cerr << worker_id << "Importing module" << std::endl;
    #endif
    // Compile the module from the received code
    #ifdef DEBUG
    std::cerr << "Loading module code of " << python_module_name << std::endl;
    std::cerr << "Code is " << python_module_code << std::endl;
    #endif
    PyObject* bytecode = Py_CompileString(python_module_code, python_module_name, Py_file_input);
    #ifdef DEBUG
    std::cerr << "Module compiled" << std::endl;
    #endif
    if(!bytecode) {
        std::cerr << "Error on module compilation:" << std::endl;
        PyErr_Print();
        return;
    }
    // Import module
    #ifdef DEBUG
    std::cerr << worker_id << "Importing module" << std::endl;
    #endif
    PyObject *pModule = PyImport_ExecCodeModule(python_module_name, bytecode);
    #ifdef DEBUG
    std::cerr << worker_id << "pModule null after bytecode import?" << (pModule == NULL) << std::endl;
    #endif
    if(!pModule) {
        std::cerr << worker_id << "Error on module import:" << std::endl;
        PyErr_Print();
        return;
    }
    #ifdef DEBUG
    std::cerr << worker_id << "Module imported" << std::endl;
    #endif
    // Disable logging at all levels
    std::string disableLoggingCmd = "import logging\nlogging.disable(logging.CRITICAL)";
    PyRun_SimpleString(disableLoggingCmd.c_str());

    // Worker loop

    bool eos = false;
    
    while (!eos) {
        // Pick next task to execute
        #ifdef DEBUG
        std::cerr << worker_id << "Waiting..." << std::endl;
        #endif
        std::unique_lock<std::mutex> lock(queueMutex);
        queueCondition.wait(lock, [] { return !taskQueue.empty(); });
        // Check the first task of the queue
        #ifdef DEBUG
        std::cerr << worker_id << "Lock acquired" << std::endl;
        #endif

        #ifdef DEBUG
        std::cerr << worker_id << "Task acquired" << std::endl;
        #endif
        std::optional<PythonParallelTask> currentTask = std::move(taskQueue.front());
        if(currentTask.has_value()) {
            taskQueue.pop_front();
        } else {
            #ifdef DEBUG
            std::cerr << worker_id << "EOS found!" << std::endl;
            #endif
            eos = true;
        }
        lock.unlock();

        #ifdef DEBUG
        std::cerr << worker_id << "eos is: " << eos << std::endl;
        #endif
        if(!eos) {
            // Load function
            PyObject *pFunc, *pData, *pArgs;
            std::string function_name = std::string(currentTask->function_name);
            #ifdef DEBUG
            std::cerr << worker_id << "Loading function " << function_name << std::endl;
            #endif
            pFunc = PyObject_GetAttrString(pModule, function_name.c_str());

            if(pFunc && PyCallable_Check(pFunc)) {
                #ifdef DEBUG
                std::cerr << worker_id << "Function " << function_name << " loaded successfully!" << std::endl;
                #endif
            } else {
                std::cerr << worker_id << "Error loading function " << function_name << std::endl;
                PyErr_Print();
                return;
            }
            pData = PyUnicode_FromString(currentTask->data);
            if(pData) {
                #ifdef DEBUG
                std::cerr << worker_id << "Data loaded successfully!" << std::endl;
                #endif
            } else {
                std::cerr << worker_id << "Error loading data" << std::endl;
                PyErr_Print();
                return;
            }

            // Execute function
            if (pData) {
                #ifdef DEBUG
                std::cerr << worker_id << "callable!" << std::endl;
                #endif
                PyObject* pArgs = PyTuple_Pack(1, pData);
                #ifdef DEBUG
                std::cerr << worker_id << "Args packed for " << function_name << ". Now calling..." << std::endl;
                #endif
                PyObject* pResult = PyObject_CallObject(pFunc, pArgs);
                #if defined(DEBUG) || defined(logftw)
                std::cerr << worker_id << "fname: " << function_name << std::endl;
                #endif
                #ifdef DEBUG
                std::cerr << worker_id << "Called " << function_name << std::endl;
                #endif
                if(pResult) {
                    const char* result = PyUnicode_AsUTF8(pResult);
                    #ifdef DEBUG
                    std::cerr << worker_id << "Setting the result as promised." << std::endl;
                    std::cerr << worker_id << "Task id is " << currentTask->id << std::endl;
                    std::cerr << worker_id << "Result size: " << strlen(result) << std::endl;
                    #endif
                    currentTask->result->set_value(PythonParallelResult(currentTask->id, result));
                    #ifdef DEBUG
                    std::cerr << worker_id << "Result has been set." << std::endl;
                    #endif
                } else {
                    std::cerr << worker_id << "Error during function execution" << std::endl;
                    PyErr_Print();
                    return;
                }
                #ifdef DEBUG
                std::cerr << worker_id << "Decreasing Python pointers" << std::endl;
                #endif
                Py_XDECREF(pResult);
                Py_XDECREF(pFunc);
                Py_XDECREF(pData);
                Py_XDECREF(pArgs);
                #ifdef DEBUG
                std::cerr << worker_id << "Python pointers decreased" << std::endl;
                #endif
            } else {
                std::cerr << worker_id << "Error loading function " << function_name << std::endl;
                PyErr_Print();
            }
        }
    }

    // Clear the loaded module
    Py_XDECREF(pModule);
    Py_XDECREF(pModuleName);
    
    // Finalize the subinterpreter
    Py_EndInterpreter(tstate);
    #ifdef DEBUG
    std::cerr << worker_id << "Interpreter ended" << std::endl;
    #endif

    // Signal that the thread ended
    {
        std::unique_lock<std::mutex> lock_end(queueMutex);
        active_threads--;
        #ifdef DEBUG
        std::cerr << "Thread " << worker_id << " ended" << std::endl;
        #endif
        cv_end.notify_all();
    }

    #ifdef DEBUG
    std::cerr << worker_id << "SubInterpreter ended and so is the worker" << std::endl;
    #endif
}

int APF_Initialize(
    bool initialize_python,
    const std::string &module_name,
    unsigned int n_workers) {
    // Read the module code
    std::string modulepath = std::filesystem::current_path().string() + "/" + module_name;
    std::ifstream python_module_file(modulepath, std::ios::binary);
    if (!python_module_file.is_open()) {
        std::cerr << "Error opening Python module file at " << modulepath << std::endl;
        return -1;
    }
    std::vector<char> python_module_buffer(std::istreambuf_iterator<char>(python_module_file), {});
    std::string module_code(python_module_buffer.data());

    if(!initialized) {
        if(initialize_python) {
            Py_Initialize();
        }
        python_module_name = strdup(module_name.c_str());
        python_module_code = strdup(module_code.c_str());
        unsigned int numThreads = n_workers;
        active_threads = numThreads;
        #ifdef DEBUG
        std::cerr << "Main thread state is " << PyGILState_GetThisThreadState() << ", interpreter is " << PyThreadState_GetInterpreter(PyGILState_GetThisThreadState()) << std::endl;
        #endif
        _save = PyEval_SaveThread();
        for (unsigned int i = 0; i < numThreads; i++) {
            std::thread(worker, i).detach();
        }
        initialized = true;
        call_count = 0;
        return 0;
    } else {
        return -1;
    }
}

std::vector<std::shared_future<PythonParallelResult>> APF_ParallelApply(
    const char* data,
    const std::vector<std::string>& function_names
) {
    // Create tasks and futures
    std::vector<std::shared_future<PythonParallelResult>> futures;
    std::vector<PythonParallelTask> tasks;
    for(auto& function_name : function_names) {
        PythonParallelTask task(call_count, strdup(data), strdup(function_name.c_str()));
        futures.push_back(std::shared_future<PythonParallelResult>(task.result->get_future()));
        tasks.emplace_back(std::move(task));
    }

    // Push tasks to the queue and wake the waiting threads
    std::unique_lock<std::mutex> lock(queueMutex);
    for (auto& task : tasks) {
        taskQueue.emplace_back(std::move(task));
    }
    lock.unlock();
    queueCondition.notify_all(); 
    // Increment the call count
    call_count++;

    // Return the futures to the caller
    return futures;
}

void APF_Finalize(bool finalize_python) {
    // Add EOS to the shared queue
    {
        std::unique_lock<std::mutex> queue_lock(queueMutex);
        taskQueue.emplace_back(std::nullopt);
        queue_lock.unlock();
        queueCondition.notify_all();
    }

    // Wait for all threads to finish
    {
        #ifdef DEBUG
        std::cerr << "Waiting for all threads to finish..." << std::endl;
        #endif
        std::unique_lock<std::mutex> lock_end(end_mutex);
        cv_end.wait(lock_end, [] { return active_threads == 0; });
        #ifdef DEBUG
        std::cerr << "All threads ended" << std::endl;
        #endif
    }

    // Reacquire the GIL and finalize Python
    PyEval_RestoreThread(_save);
    /*if(finalize_python) {
        auto finalization_code = Py_FinalizeEx();
        if(finalization_code < 0) {
            std::cerr << "Error finalizing Python" << std::endl;
        } else {
            std::cerr << "Python finalized correctly" << std::endl;
        }
    }*/
}
