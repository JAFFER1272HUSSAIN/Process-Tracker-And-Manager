# sjf.py

def sjf_non_preemptive(data):
    # Assuming `data` is a list of tuples (process_id, arrival_time, burst_time)
   
    # Sort processes by arrival time
    processes = sorted(data, key=lambda x: x[1])
    
    start_time = 0
    schedule = []
    waiting_list = []
    n = len(data)
    waiting_times = [0] * n
    turnaround_times = [0] * n

    while processes or waiting_list:
        while processes and processes[0][1] <= start_time:
            waiting_list.append(processes.pop(0))
        waiting_list.sort(key=lambda x: x[2])  # Sort by burst time
        if waiting_list:
            pid, arrival, burst = waiting_list.pop(0)
            start_time = max(start_time, arrival)
            finish_time = start_time + burst
            schedule.append((pid, start_time, finish_time))
            waiting_times[pid - 1] = start_time - arrival
            turnaround_times[pid - 1] = finish_time - arrival
            start_time = finish_time
        else:
            start_time += 1

    avg_wt = sum(waiting_times) / n
    avg_tat = sum(turnaround_times) / n

    return schedule, avg_wt, avg_tat
from queue import PriorityQueue

class Process:
    def __init__(self, pid, arrival_time, burst_time):
        self.pid = pid
        self.arrival_time = arrival_time
        self.burst_time = burst_time

def sjf_preemptive(data):
    processes = [Process(pid, arrival, burst) for pid, arrival, burst in sorted(data, key=lambda x: x[1])]
    
    time = 0
    schedule = []
    waiting_queue = PriorityQueue()
    remaining_burst_times = {process.pid: process.burst_time for process in processes}
    current_process = None

    while processes or not waiting_queue.empty() or current_process is not None:
        # Add arriving processes to the waiting queue
        while processes and processes[0].arrival_time <= time:
            process = processes.pop(0)
            waiting_queue.put((process.burst_time, process.arrival_time, process.pid))

        if current_process:
            pid, start_time, _ = current_process
            remaining_burst = remaining_burst_times[pid]
            if remaining_burst == 0:
                # Current process is finished
                finish_time = time
                schedule.append((pid, start_time, finish_time))
                current_process = None
            elif not waiting_queue.empty() and waiting_queue.queue[0][0] < remaining_burst:
                # Preempt current process if a shorter one arrives
                waiting_queue.put((remaining_burst, start_time, pid))
                current_process = None

        if not current_process and not waiting_queue.empty():
            # Start executing the next process from the waiting queue
            remaining_burst, arrival, pid = waiting_queue.get()
            if remaining_burst_times[pid] == remaining_burst:
                start_time = time
            else:
                start_time = time - (remaining_burst - remaining_burst_times[pid])
            current_process = (pid, start_time, remaining_burst)
            remaining_burst_times[pid] -= 1

        time += 1

    return schedule
