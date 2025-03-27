def first_fit(blocks, processes):
    allocation = [-1] * len(processes)

    for i, process in enumerate(processes):
        for j, block in enumerate(blocks):
            if block >= process:
                allocation[i] = j
                blocks[j] -= process
                break

    return allocation
