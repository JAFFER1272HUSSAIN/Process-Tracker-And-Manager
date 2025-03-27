def best_fit(processes, memory_blocks):
    # Sort memory blocks in ascending order
    memory_blocks.sort()
    allocated = []
    process_names = [f'P{i+1}' for i in range(len(processes))]
    block_names = [f'B{i+1}' for i in range(len(memory_blocks))]
    named_memory_blocks = list(zip(memory_blocks, block_names))

    for i, process in enumerate(processes):
        # Find the smallest block that can fit the process
        for j, (block, block_name) in enumerate(named_memory_blocks):
            if block >= process:
                allocated.append((process_names[i], block_name))
                named_memory_blocks.pop(j)
                break
    return allocated

def best_fit_new(processes, memory_blocks):
    # Initialize remaining space in each block
    block_names = [f'B{i+1}' for i in range(len(memory_blocks))]
    named_memory_blocks = {block_name: block for block, block_name in zip(memory_blocks, block_names)}
    allocated = []
    process_names = [f'P{i+1}' for i in range(len(processes))]

    for i, process in enumerate(processes):
        # Sort the blocks by remaining space in ascending order
        sorted_blocks = sorted(named_memory_blocks.items(), key=lambda item: item[1])
        for block_name, space in sorted_blocks:
            if space >= process:
                allocated.append((process_names[i], block_name))
                named_memory_blocks[block_name] -= process
                break
    return allocated

def worst_fit_new(processes, memory_blocks):
    # Initialize remaining space in each block
    block_names = [f'B{i+1}' for i in range(len(memory_blocks))]
    named_memory_blocks = {block_name: block for block, block_name in zip(memory_blocks, block_names)}
    allocated = []
    process_names = [f'P{i+1}' for i in range(len(processes))]

    for i, process in enumerate(processes):
        # Sort the blocks by remaining space in descending order
        sorted_blocks = sorted(named_memory_blocks.items(), key=lambda item: item[1], reverse=True)
        for block_name, space in sorted_blocks:
            if space >= process:
                allocated.append((process_names[i], block_name))
                named_memory_blocks[block_name] -= process
                break
    return allocated

def worst_fit(processes, memory_blocks):
    # Sort memory blocks in descending order
    memory_blocks.sort(reverse=True)
    allocated = []
    process_names = [f'P{i+1}' for i in range(len(processes))]
    block_names = [f'B{i+1}' for i in range(len(memory_blocks))]
    named_memory_blocks = list(zip(memory_blocks, block_names))

    for i, process in enumerate(processes):
        # Find the largest block that can fit the process
        for j, (block, block_name) in enumerate(named_memory_blocks):
            if block >= process:
                allocated.append((process_names[i], block_name))
                named_memory_blocks.pop(j)
                break
    return allocated

def first_fit(processes, memory_blocks):
    allocated = []
    process_names = [f'P{i+1}' for i in range(len(processes))]
    block_names = [f'B{i+1}' for i in range(len(memory_blocks))]
    named_memory_blocks = list(zip(memory_blocks, block_names))

    for i, process in enumerate(processes):
        # Find the first block that can fit the process
        for j, (block, block_name) in enumerate(named_memory_blocks):
            if block >= process:
                allocated.append((process_names[i], block_name))
                named_memory_blocks.pop(j)
                break
    return allocated

def first_fit_new(processes, memory_blocks):
    allocated = []
    process_names = [f'P{i+1}' for i in range(len(processes))]
    block_names = [f'B{i+1}' for i in range(len(memory_blocks))]
    named_memory_blocks = list(zip(memory_blocks, block_names))
    remaining_space = {block_name: block for block, block_name in named_memory_blocks}

    for i, process in enumerate(processes):
        for block_name, space in remaining_space.items():
            if space >= process:
                allocated.append((process_names[i], block_name))
                remaining_space[block_name] -= process
                break
    return allocated

def main():
    # Example usage:
    processes = [2, 5, 1, 8, 7]
    memory_blocks = [6, 2, 7, 2, 9]

    print("Best Fit:", best_fit_new(processes.copy(), memory_blocks.copy()))
    print("Worst Fit:", worst_fit_new(processes.copy(), memory_blocks.copy()))
    print("First Fit:", first_fit_new(processes.copy(), memory_blocks.copy()))

if __name__ == "__main__":
    main()
