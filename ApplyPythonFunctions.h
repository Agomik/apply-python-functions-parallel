#include <future>
#include <vector>
#include "APF_Types.h"

int APF_Initialize(
    bool init_python,
    const std::string &python_module_name,
    unsigned int n_workers
);

std::vector<std::shared_future<PythonParallelResult>> APF_ParallelApply(
    const char* data,
    const std::vector<std::string>& function_names
);

void APF_Finalize(
    bool finalize_python
);