#include <chrono>
#include <future>

// The Python task result
struct PythonParallelResult {
    const unsigned short id;
    const char* value;
    const std::chrono::system_clock::time_point time;

    PythonParallelResult(
        const unsigned short id,
        const char* value)
        : id(id),
          value(value),
          time(std::chrono::system_clock::now()) {}
};

// The Python task to be executed
struct PythonParallelTask {
    const unsigned short id;
    const char* data;
    const char* function_name;
    std::unique_ptr<std::promise<PythonParallelResult>> result;

    PythonParallelTask(
        const unsigned short id,
        const char* data,
        const char* function_name)
        : id(id),
          data(data),
          function_name(function_name),
          result(std::make_unique<std::promise<PythonParallelResult>>()) {}
};