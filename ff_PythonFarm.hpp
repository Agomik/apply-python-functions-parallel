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
#include "APF_Types.h"
#include "ff/ff.hpp"

using namespace ff;

class ff_PythonFarm_worker : public ff_node_t<PythonParallelTask, PythonParallelResult> {
private:
  int worker_id;
  PyObject* pModuleName;
  PyObject* pModule;
  PyThreadState *tstate;
  const char * python_module_code;
  const char * python_module_name;
  PyObject* bytecode;
public: 
  ff_PythonFarm_worker(
    int workder_id,
    const char* python_module_name,
    const char* python_module_code)
    : worker_id(workder_id),
      python_module_code(python_module_code) {

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
    tstate = NULL;
    // Create new subinterpreter (automatically releases the main interpreter's GIL and acquires the new subinterpreter's)
    PyStatus status = Py_NewInterpreterFromConfig(&tstate, &config);
    #ifdef DEBUG
    std::cerr << "Subinterpreter started in thread " << worker_id << std::endl;
    std::cerr << worker_id << "interpreter is: " << PyThreadState_GetInterpreter(tstate) << ", tstate is " << PyThreadState_Get() << std::endl;
    #endif
    if (PyStatus_Exception(status)) {
        PyGILState_Release(gstate);
        throw "Error creating interpreter in thread.";
    }
    // Adds the cwd to the (sub)interpreter's Python path for the subinterpreter
    auto cwd = std::filesystem::current_path().string();
    #ifdef DEBUG
    std::cerr << worker_id << "cwd is " << cwd << std::endl;
    #endif
    std::string sysPathCmd = "import sys\nsys.path.append('" + cwd + "')";
    PyRun_SimpleString(sysPathCmd.c_str());
    if (PyStatus_Exception(status)) {
        PyGILState_Release(gstate);
        throw "Error creating interpreter in thread.";
    } else {
        #ifdef DEBUG
        std::cerr << "Cwd added to path for loading modules." << std::endl;
        #endif
    }
    // Load the Python module
    pModuleName = PyUnicode_FromString(python_module_name);
    #ifdef DEBUG
    std::cerr << worker_id << "Importing module" << python_module_name << std::endl;
    #endif
    // Compile the module from the received code
    #ifdef DEBUG
    std::cerr << "Loading module code of " << python_module_name << std::endl;
    std::cerr << "Code is " << python_module_code << std::endl;
    #endif
    bytecode = Py_CompileString(python_module_code, python_module_name, Py_file_input);
    #ifdef DEBUG
    std::cerr << "Module compiled" << std::endl;
    #endif
    if(!bytecode) {
        PyErr_Print();
        throw "Error on module compilation.";
    }
    // Import module
    #ifdef DEBUG
    std::cerr << worker_id << "Importing module" << std::endl;
    #endif
    pModule = PyImport_ExecCodeModule(python_module_name, bytecode);
    #ifdef DEBUG
    std::cerr << worker_id << "pModule null after bytecode import?" << (pModule == NULL) << std::endl;
    #endif
    if(!pModule) {
        PyErr_Print();
        throw "Error on module import in worker " + worker_id;
    }
    #ifdef DEBUG
    std::cerr << worker_id << "Module imported" << std::endl;
    #endif
    // Disable logging at all levels
    std::string disableLoggingCmd = "import logging\nlogging.disable(logging.CRITICAL)";
    PyRun_SimpleString(disableLoggingCmd.c_str());
    tstate = PyEval_SaveThread();
    #ifdef DEBUG
    std::cerr << worker_id << "Worker initialization complete" << std::endl;
    #endif
  }

  ~ff_PythonFarm_worker() {
    // Wait for all threads to finish
    {
        #ifdef DEBUG
        std::cerr << worker_id << "Destroying worker" << std::endl;
        #endif
        // Clear the loaded module
        Py_XDECREF(pModule);
        Py_XDECREF(pModuleName);

        // Finalize the subinterpreter
        Py_EndInterpreter(tstate);
        #ifdef DEBUG
        std::cerr << worker_id << "Interpreter ended" << std::endl;
        #endif
    }

  }

  PythonParallelResult * svc(PythonParallelTask * currentTask) {
    #ifdef DEBUG
    std::cerr << worker_id << "Task received! " << std::endl;
    #endif
    #if defined(DEBUG) || defined(logftw)
    std::cerr << worker_id << "fname: " << currentTask->function_name << std::endl;
    #endif
    PyEval_RestoreThread(tstate);
    // Load function
    PyObject *pFunc, *pData, *pArgs;
    std::string function_name = std::string(currentTask->function_name);
    #ifdef DEBUG
    std::cerr << worker_id << "tstate is " << tstate << std::endl;
    std::cerr << worker_id << "Loading function " << function_name.c_str() << " from module " << pModule << std::endl;
    #endif
    pFunc = PyObject_GetAttrString(pModule, function_name.c_str());
    #ifdef DEBUG
    std::cerr << worker_id << "Function is: " << pFunc << std::endl;
    #endif
    if(pFunc && PyCallable_Check(pFunc)) {
        #ifdef DEBUG
        std::cerr << worker_id << "Function " << function_name << " loaded successfully!" << std::endl;
        #endif
    } else {
        std::cerr << worker_id << "Error loading function " << function_name << std::endl;
        PyErr_Print();
        return GO_ON;
    }
    pData = PyUnicode_FromString(currentTask->data);
    if(pData) {
        #ifdef DEBUG
        std::cerr << worker_id << "Data loaded successfully!" << std::endl;
        #endif
    } else {
        std::cerr << worker_id << "Error loading data" << std::endl;
        PyErr_Print();
        return GO_ON;
    }

    // Execute function
    if (pData) {
        #ifdef DEBUG
        std::cerr << worker_id << "callable!" << std::endl;
        #endif
        PyObject* pArgs = PyTuple_Pack(1, pData);
        #ifdef DEBUG
        std::cerr << worker_id << "tstate is " << tstate << std::endl;
        std::cerr << worker_id << "Args packed for " << function_name << ". Now calling..." << std::endl;
        #endif
        PyObject* pResult = PyObject_CallObject(pFunc, pArgs);
        #ifdef DEBUG
        std::cerr << worker_id << "Called " << function_name << std::endl;
        #endif
        PythonParallelResult *taskResult;
        if(pResult) {
            const char* result = PyUnicode_AsUTF8(pResult);
            #ifdef DEBUG
            std::cerr << worker_id << "Setting the result as promised." << std::endl;
            std::cerr << worker_id << "Task id is " << currentTask->id << std::endl;
            std::cerr << worker_id << "Result size: " << strlen(result) << std::endl;
            #endif
            PythonParallelTask task = std::move(*currentTask);
            task.result->set_value(PythonParallelResult(task.id, result));
            #ifdef DEBUG
            std::cerr << worker_id << "Result has been set." << std::endl;
            #endif
        } else {
            std::cerr << worker_id << "Error during function execution" << std::endl;
            PyErr_Print();
            return GO_ON;
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
        tstate = PyEval_SaveThread();

        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        return GO_ON;
    } else {
        std::cerr << worker_id << "Error loading function " << function_name << std::endl;
        PyErr_Print();
        tstate = PyEval_SaveThread();
        return GO_ON;
    }
    /*#ifdef DEBUG
    std::cerr << worker_id << "Setting value" << std::endl;
    #endif
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    currentTask->result->set_value(PythonParallelResult(0, "hello"));
    #ifdef DEBUG
    std::cerr << worker_id << "Returning" << std::endl;
    #endif
    return GO_ON;*/
  }
};

// Emitter
class ff_PythonFarm_emitter : public ff_monode_t<PythonParallelTask, PythonParallelTask> {};
class ff_PythonFarm_async_emitter : public ff_PythonFarm_emitter {

private: 
    int m;
    std::deque<std::optional<PythonParallelTask>> taskQueue;
    std::mutex queueMutex;
    std::condition_variable queueCondition;
    unsigned int call_count;

public:
    ff_PythonFarm_async_emitter()
    : call_count(0) {}

    // Enqueue the tasks (thread-safe)
    std::vector<std::shared_future<PythonParallelResult>> apply(const char* data, const std::vector<std::string> &function_names) {
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
    
    // Signals the end of the tasks (thread-safe)
    void send_eos() {
        std::unique_lock<std::mutex> queue_lock(queueMutex);
        taskQueue.emplace_back(std::nullopt);
        queue_lock.unlock();
        queueCondition.notify_all();
    }

    PythonParallelTask * svc(PythonParallelTask * task) {
        bool eos_received = false;
        std::deque<std::optional<PythonParallelTask>> currentTasks;
        while(!eos_received) {
            // Pick next task to execute
            #ifdef DEBUG
            std::cerr << "EWaiting..." << std::endl;
            #endif
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCondition.wait(lock, [this] { return !taskQueue.empty(); });
            // Check the first task of the queue
            #ifdef DEBUG
            std::cerr << "ELock acquired" << std::endl;
            #endif
            while(!taskQueue.empty()) {
                currentTasks.emplace_back(std::move(taskQueue.front()));
                taskQueue.pop_front();
            }
            lock.unlock();
            
            while(!currentTasks.empty() && currentTasks.front().has_value()) {
                PythonParallelTask *currentTask = new PythonParallelTask(currentTasks.front()->id, currentTasks.front()->data, currentTasks.front()->function_name);
                currentTask->result = std::move(currentTasks.front()->result);
                currentTasks.pop_front();
                #ifdef DEBUG
                std::cerr << "ESending task for " << currentTask->function_name << std::endl;
                #endif
                ff_send_out(currentTask);
                #ifdef DEBUG
                std::cerr << "ETask sent" << std::endl;
                #endif
            } 
            if(!currentTasks.empty() && !currentTasks.front().has_value()) {
                #ifdef DEBUG
                std::cerr << "EEmitter is going to send the ff EOS" << std::endl;
                #endif
                eos_received = true;
            }
            #ifdef DEBUG
            std::cerr << "EIteration ended" << std::endl;
            //std::this_thread::sleep_for(std::chrono::milliseconds(5000));
            #endif
        }

        #ifdef DEBUG
        std::cerr << "ESending EOS" << std::endl;
        #endif
        return EOS;
    };

    ~ff_PythonFarm_async_emitter() {
        #ifdef DEBUG
        std::cerr << "ECalling emitter's destroyer" << std::endl;
        #endif
    };
};

class ff_PythonFarm_collector : public ff_minode_t<PythonParallelTask> {
public:
    PythonParallelTask* svc(PythonParallelTask *task) {
        #ifdef DEBUG
        std::cerr << "CReceived task" << task->function_name << std::endl;
        #endif
        return GO_ON;
    }
};

// Must be created after Py_Initialize()
class ff_PythonFarm : public ff_farm {
private:
    int nw;
    PyThreadState *_save;
public:
    ff_PythonFarm(int nw, std::string python_module_name) : nw(nw) {
        #ifdef DEBUG
        std::cerr << "Initializing farm" << std::endl;
        #endif
        // Read the module code
        std::string modulepath = std::filesystem::current_path().string() + "/" + python_module_name;
        std::ifstream python_module_file(modulepath, std::ios::binary);
        if (!python_module_file.is_open()) {
            throw std::invalid_argument("Error opening module file at " + modulepath);
        }
        std::vector<char> python_module_buffer(std::istreambuf_iterator<char>(python_module_file), {});
        std::string loaded_module_code(python_module_buffer.data());

        _save = PyEval_SaveThread();

        // Initialize the workers
        std::vector<ff_node*> workers;
        for(int i = 0; i < nw; i++) {
            //workers.emplace_back(std::make_unique<ff_PythonFarm_worker>(i, strdup(python_module_name.c_str()), strdup(loaded_module_code.c_str())));
            workers.emplace_back(new ff_PythonFarm_worker(i, strdup(python_module_name.c_str()), strdup(loaded_module_code.c_str())));
        }
        ff_farm::add_workers(workers);
        //ff_farm::add_emitter(NULL); 
        //ff_farm::add_collector(NULL);

        #ifdef DEBUG
        std::cerr << "Farm initialized" << std::endl;
        #endif
    }

    ~ff_PythonFarm() {
        #ifdef DEBUG
        std::cerr << "Destroying farm" << std::endl;
        #endif
        PyEval_RestoreThread(_save);
    }
};