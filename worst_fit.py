def worst_fit(blocks, processes):
    allocation = [-1] * len(processes)

    for i, process in enumerate(processes):
        worst_idx = -1
        for j, block in enumerate(blocks):
            if block >= process:
                if worst_idx == -1 or blocks[worst_idx] < block:
                    worst_idx = j

        if worst_idx != -1:
            allocation[i] = worst_idx
            blocks[worst_idx] -= process

    return allocation
