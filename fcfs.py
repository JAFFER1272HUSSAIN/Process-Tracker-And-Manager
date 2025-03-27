def fcfs_non_preemptive(data):
    # Assuming `data` is a list of tuples (process_id, arrival_time, burst_time)
    processes = sorted(data, key=lambda x: x[1])  # Sort by arrival time
    start_time = 0
    schedule = []

    for pid, arrival, burst in processes:
        start_time = max(start_time, arrival)
        finish_time = start_time + burst
        schedule.append((pid, start_time, finish_time))
        start_time = finish_time

    return schedule
