import argparse

# Parse configuration
def parse_config(file_name):
    config = file_name.split("_")[1:]
    config[-1] = config[-1].split('.')[0]
    nodes = int(config[0])
    cores_per_node = int(config[1])
    memslices_per_node = int(config[2])
    num_applications = int(config[3])
    cap_func = bool(config[4])
    allocations = int(config[5])
    steps = int(config[6])
    print(f"Results config: nodes={nodes}, cores_per_node={cores_per_node}, memslices_per_node={memslices_per_node}, "
            f"num_applications={num_applications}, cap_func={cap_func}, allocations={allocations}, steps={steps}")

    # Check against actual config
    with open(file_name, 'r') as f:
        lines = f.read().split('\n')
    l = [l for l in lines if "Creating a simulation with parameters" in l][0]
    actual_config = l.split(":")[-1]
    actual_config = actual_config.split(" ")
    assert(nodes == int(actual_config[1].split("=")[-1][:-1]))
    assert(cores_per_node == int(actual_config[2].split("=")[-1][:-1]))
    assert(memslices_per_node == int(actual_config[3].split("=")[-1]))
    assert(num_applications == int(actual_config[4].split("=")[-1][:-1]))
    assert(cap_func == bool(actual_config[7].split("=")[-1]))
    assert(allocations == int(actual_config[5].split("=")[-1][:-1]))
    return (nodes, cores_per_node, memslices_per_node, num_applications, cap_func, allocations, steps)

def parse_step_data(file_name, steps):
    # Search for timing and variables
    var_counts = []
    solver_times = []

    with open(file_name, 'r') as f:
        lines = f.read().split('\n')

    for l in lines:
        if l.startswith("#Variables:"):
            var_counts.append(l)
        elif "Solver has run successfully in" in l:
            solver_times.append(l)
    # For some reason, this line is printed twice
    assert(len(var_counts) == 2*steps)
    assert(len(solver_times) == steps)

    # Parse variable numbers
    var_counts = [int(v.split(" ")[1]) for (i, v) in enumerate(var_counts) if i % 2 == 1]
    solver_times = [int(t.split(" ")[-3][:-3]) for t in solver_times]
    return var_counts, solver_times

if __name__ == "__main__":
    # Handle arguments
    parser = argparse.ArgumentParser(description='Parse results file')
    parser.add_argument('file_name', type=str,
                        help='Name of the file to parse')
    args = parser.parse_args()
    (nodes, cores_per_node, memslices_per_node, num_applications, cap_func, allocations, steps) = parse_config(args.file_name)
    var_counts, solver_times = parse_step_data(args.file_name, steps)

    var_avg = sum(var_counts) / len(var_counts)
    var_avg = round(var_avg, 2)
    var_counts = [str(t) for t in var_counts]
    var_str = ", ".join(var_counts)
    print(f"{nodes}, {cores_per_node}, {memslices_per_node}, {num_applications}, {cap_func}, {allocations}, {steps},  {var_str}, {var_avg}")

    solver_avg = sum(solver_times) / len(solver_times)
    solver_avg = round(solver_avg, 2)
    solver_times = [str(t) for t in solver_times]
    solver_str = ", ".join(solver_times)
    print(f"{nodes}, {cores_per_node}, {memslices_per_node}, {num_applications}, {cap_func}, {allocations}, {steps}, {solver_str}, {solver_avg}")