def best_fit(blocks, processes):
    allocation = [-1] * len(processes)

    for i, process in enumerate(processes):
        best_idx = -1
        for j, block in enumerate(blocks):
            if block >= process:
                if best_idx == -1 or blocks[best_idx] > block:
                    best_idx = j

        if best_idx != -1:
            allocation[i] = best_idx
            blocks[best_idx] -= process

    return allocation
