import sys, time, logging, statistics, test_functions

start = time.time_ns()

input_file = sys.argv[2]
input_functions = []
for input_f in sys.argv[4:]:
    input_functions.append(eval(sys.argv[1] + "." + input_f))
iterations = int(sys.argv[3])
times = []
logging.disable(logging.CRITICAL)

test_ready = time.time_ns()
for i in range(iterations):
    with open(input_file, 'r') as file:
        data = file.read()
        for f in input_functions:
            f(data)
        stop = time.time_ns()
        times.append(stop)
stop = time.time_ns()

print("Completion time: " + str(int((stop - start)/1000)))
print("Initialization overhead: " + str(int((test_ready - start)/1000)))
print("Computation time: " + str(int((stop - test_ready)/1000)))
service_times = []
if iterations > 1:
    for i in range(len(times)-1):
        duration = times[i+1] - times[i]
        service_times.append(duration)
        # print("Service time " + str(i) + ": " + str(int(duration/10)))
    print("Average service time: " + str(int(statistics.mean(service_times)/1000)))