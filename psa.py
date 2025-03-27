def psa_non_preemptive(data):
    # Assuming `data` is a list of tuples (process_id, arrival_time, burst_time, priority)
    processes = sorted(data, key=lambda x: (x[1], -x[3]))  # Sort by arrival time and priority (higher priority first)
    start_time = 0
    schedule = []
    waiting_list = []

    while processes:
        while processes and processes[0][1] <= start_time:
            waiting_list.append(processes.pop(0))
        if waiting_list:
            waiting_list.sort(key=lambda x: x[3], reverse=True)  # Sort by priority (higher priority first)
            pid, arrival, burst, priority = waiting_list.pop(0)
            start_time = max(start_time, arrival)
            finish_time = start_time + burst
            schedule.append((pid, start_time, finish_time))
            start_time = finish_time
        else:
            start_time = processes[0][1]  # Move to the next arrival time

    return schedule


from queue import PriorityQueue

def psa_preemptive(data):
    # Assuming `data` is a list of tuples (process_id, arrival_time, burst_time, priority)

    # Assuming `data` is a list of tuples (process_id, arrival_time, burst_time, priority)
    
    processes = sorted(data, key=lambda x: (x[1], -x[3]))  # Sort processes by arrival time and priority
    waiting_queue = PriorityQueue()
    schedule = []
    time = 0
    current_process = None

    while processes or not waiting_queue.empty() or current_process is not None:
        while processes and processes[0][1] <= time:
            pid, arrival, burst, priority = processes.pop(0)
            waiting_queue.put((priority, pid, burst, arrival))

        if current_process:
            pid, start_time, remaining_burst, _ = current_process
            if remaining_burst == 0:
                finish_time = time
                schedule.append((pid, start_time, finish_time))
                current_process = None
            elif not waiting_queue.empty() and waiting_queue.queue[0][0] < current_process[0]:
                waiting_queue.put(current_process)
                current_process = None

        if not current_process and not waiting_queue.empty():
            priority, pid, burst, arrival = waiting_queue.get()
            start_time = time
            remaining_burst = burst - 1
            current_process = (pid, start_time, remaining_burst, priority)

        time += 1
        if current_process:
            current_process = (current_process[0], current_process[1], current_process[2] - 1, current_process[3])

    return schedule