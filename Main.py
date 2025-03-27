import os
import curses
import fcfs
import sjf
import rr
import psa
import first_fit
import best_fit
import worst_fit
import memoryScheduling as mm

input_file_path = "C:\\Users\\HP\\Downloads\\Project\\"


def clear_screen(stdscr):
    stdscr.clear()
    stdscr.refresh()

def print_menu(stdscr, options, selected_index, title):
    clear_screen(stdscr)
    h, w = stdscr.getmaxyx()
    title_x = w // 2 - len(title) // 2
    stdscr.addstr(1, title_x, title, curses.A_BOLD | curses.A_UNDERLINE)

    for i, option in enumerate(options):
        x = w // 2 - len(option) // 2
        y = h // 2 - len(options) // 2 + i
        if i == selected_index:
            stdscr.addstr(y, x, f"--> {option}", curses.A_REVERSE | curses.color_pair(1))
        else:
            stdscr.addstr(y, x, option, curses.color_pair(2))
    
    instruction = "Use Up/Down arrow keys to navigate. Press Enter to select."
    stdscr.addstr(h-2, w//2 - len(instruction)//2, instruction, curses.A_DIM)
    stdscr.refresh()

def select_preemptive_non_preemptive(stdscr, algorithm):
    options = ["Preemptive", "Non-Preemptive"]
    selected_index = 0

    while True:
        print_menu(stdscr, options, selected_index, f"{algorithm} - Preemption Mode")
        key = stdscr.getch()

        if key == curses.KEY_UP:
            selected_index = (selected_index - 1) % len(options)
        elif key == curses.KEY_DOWN:
            selected_index = (selected_index + 1) % len(options)
        elif key == curses.KEY_ENTER or key in [10, 13]:
            stdscr.clear()
            mode = options[selected_index]
            stdscr.addstr(0, 0, f"You selected {algorithm} with {mode}.")
            stdscr.refresh()
            stdscr.getch()
            select_file_and_run_algorithm(stdscr, algorithm, mode)
            break

def select_file_and_run_algorithm(stdscr, algorithm, mode):
    if (algorithm == "First-Come, First-Served (FCFS)" and mode == "Preemptive") or (algorithm == "Round Robin (RR)" and mode == "Non-Preemptive"):
        stdscr.addstr(0, 0, f"{mode} mode not available for {algorithm}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()
        return
    
    stdscr.addstr(1, 0, "Enter the path to the input file: ")
    curses.echo()
    input_file = stdscr.getstr(2, 0).decode('utf-8')
    stdscr.clear()

    if not os.path.exists(input_file):
        stdscr.addstr(0, 0, "File not found. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()
        return

    try:
        with open(input_file, 'r') as file:
            data = [tuple(map(int, line.split())) for line in file.readlines()]
    except Exception as e:
        stdscr.addstr(0, 0, f"Error reading input file: {e}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()
        return

    try:
        if algorithm == "First-Come, First-Served (FCFS)":
            output = fcfs.fcfs_non_preemptive(data)
        elif algorithm == "Shortest Job First (SJF)":
            if mode == "Preemptive":
                output = sjf.sjf_preemptive(data)
            else:
                output = sjf.sjf_non_preemptive(data)
        elif algorithm == "Round Robin (RR)":
            quantum = 2
            output = rr.rr_preemptive(data, quantum)
        elif algorithm == "Priority Scheduling Algorithm (PSA)":
            if mode == "Preemptive":
                output = psa.psa_preemptive(data)
            else:
                output = psa.psa_non_preemptive(data)

        output_file = f"output_{algorithm.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(',', '')}_{mode.lower()}.txt"
        with open(output_file, 'w') as file:
            for item in output:
                file.write(f"{item}\n")

        # Display output on console
        stdscr.clear()
        stdscr.addstr(0, 0, f"Algorithm {algorithm} with {mode} ran successfully. Output:\n")
        for idx, item in enumerate(output, start=1):
            stdscr.addstr(idx, 0, f"{item}")
        stdscr.addstr(len(output) + 1, 0, f"\nOutput stored in {output_file}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()

    except Exception as e:
        stdscr.addstr(0, 0, f"Error running algorithm or writing output file: {e}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()

def algorithm_menu(stdscr):
    algorithms = ["First-Come, First-Served (FCFS)", "Shortest Job First (SJF)", "Round Robin (RR)", "Priority Scheduling Algorithm (PSA)"]
    selected_index = 0

    while True:
        print_menu(stdscr, algorithms, selected_index, "Algorithm Selection Menu")
        key = stdscr.getch()

        if key == curses.KEY_UP:
            selected_index = (selected_index - 1) % len(algorithms)
        elif key == curses.KEY_DOWN:
            selected_index = (selected_index + 1) % len(algorithms)
        elif key == curses.KEY_ENTER or key in [10, 13]:
            select_preemptive_non_preemptive(stdscr, algorithms[selected_index])
            break

def memory_management_menu(stdscr):
    memory_options = ["First-Fit", "Best-Fit", "Worst-Fit"]
    selected_index = 0

    while True:
        print_menu(stdscr, memory_options, selected_index, "Memory Management Menu")
        key = stdscr.getch()

        if key == curses.KEY_UP:
            selected_index = (selected_index - 1) % len(memory_options)
        elif key == curses.KEY_DOWN:
            selected_index = (selected_index + 1) % len(memory_options)
        elif key == curses.KEY_ENTER or key in [10, 13]:
            select_file_and_run_memory_management(stdscr, memory_options[selected_index])
            break

def select_file_and_run_memory_management(stdscr, option):
    stdscr.clear()
    stdscr.addstr(1, 0, "Enter the path to the input file: ")
    curses.echo()
    global input_file_path
    input_file = input_file_path
    input_file += stdscr.getstr(2, 0).decode('utf-8')
    stdscr.clear()

    if not os.path.exists(input_file):
        stdscr.addstr(0, 0, "File not found. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()
        return

    try:
        with open(input_file, 'r') as file:
            lines = file.readlines()
            processes = list(map(int, lines[0].split(' ')))
            blocks = list(map(int, lines[1].split(' ')))
    
    except Exception as e:
        stdscr.addstr(0, 0, f"Error reading input file: {e}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()
        return

    try:
        if option == "First-Fit":
            output = mm.first_fit(processes, blocks)
        elif option == "Best-Fit":
            output = mm.best_fit(processes, blocks)
        elif option == "Worst-Fit":
            output = mm.worst_fit(processes, blocks)

        output_file = f"output_{option.lower().replace('-', '_')}.txt"
        with open(output_file, 'w') as file:
            for item in output:
                file.write(f"{item}\n")

        # Display output on console
        stdscr.clear()
        stdscr.addstr(0, 0, f"Memory Management {option} ran successfully. Output:\n")
        for item in range(len(output)):
            p = output[item][0]
            b = output[item][1]
            stdscr.addstr(item+1, 0, f"Process {p}: Block {b if b != -1 else 'Not allocated'}")
        
        stdscr.addstr(len(output) + 2, 0, f"\nOutput stored in {output_file}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()

    except Exception as e:
        stdscr.addstr(0, 0, f"Error running memory management or writing output file: {e}. Press any key to return.")
        stdscr.refresh()
        stdscr.getch()

def main(stdscr):
    curses.curs_set(0)
    curses.start_color()
    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)
    curses.init_pair(2, curses.COLOR_CYAN, curses.COLOR_BLACK)

    main_options = ["Algorithm Selection", "Memory Management", "Exit"]
    selected_index = 0

    while True:
        print_menu(stdscr, main_options, selected_index, "Main Menu")
        key = stdscr.getch()

        if key == curses.KEY_UP:
            selected_index = (selected_index - 1) % len(main_options)
        elif key == curses.KEY_DOWN:
            selected_index = (selected_index + 1) % len(main_options)
        elif key == curses.KEY_ENTER or key in [10, 13]:
            if main_options[selected_index] == "Algorithm Selection":
                algorithm_menu(stdscr)
            elif main_options[selected_index] == "Memory Management":
                memory_management_menu(stdscr)
            elif main_options[selected_index] == "Exit":
                clear_screen(stdscr)
                stdscr.addstr("Exiting the program.\n")
                stdscr.refresh()
                break

if __name__ == "__main__":
    curses.wrapper(main)
