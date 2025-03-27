def rr_preemptive(data, time_quantum):
    # Assuming `data` is a list of tuples (process_id, arrival_time, burst_time)
    from collections import deque

    class Process:
        def __init__(self, pid, arrival_time, burst_time):
            self.pid = pid
            self.arrival_time = arrival_time
            self.burst_time = burst_time
            self.burst_time_remaining = burst_time
            self.completion_time = 0
            self.turnaround_time = 0
            self.waiting_time = 0
            self.is_complete = False
            self.in_queue = False

    def check_for_new_arrivals(processes, current_time, ready_queue):
        for process in processes:
            if process.arrival_time <= current_time and not process.in_queue and not process.is_complete:
                process.in_queue = True
                ready_queue.append(process)

    processes = [Process(pid, arrival, burst) for pid, arrival, burst in sorted(data, key=lambda x: x[1])]
    ready_queue = deque()
    current_time = 0
    schedule = []

    # Initially add the first process to the ready queue
    check_for_new_arrivals(processes, current_time, ready_queue)

    while ready_queue:
        current_process = ready_queue.popleft()
        
        # Determine execution time
        execution_time = min(current_process.burst_time_remaining, time_quantum)
        start_time = current_time
        finish_time = current_time + execution_time
        current_time = finish_time

        # Update remaining burst time
        current_process.burst_time_remaining -= execution_time

        if current_process.burst_time_remaining > 0:
            # Process is not complete, re-add to queue
            check_for_new_arrivals(processes, current_time, ready_queue)
            ready_queue.append(current_process)
        else:
            # Process is complete
            current_process.is_complete = True
            current_process.completion_time = current_time
            current_process.turnaround_time = current_process.completion_time - current_process.arrival_time
            current_process.waiting_time = current_process.turnaround_time - current_process.burst_time
            schedule.append((current_process.pid, start_time, finish_time))
            check_for_new_arrivals(processes, current_time, ready_queue)

    return schedule

