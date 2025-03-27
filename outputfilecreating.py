#output_file = f"new.txt"
#output = "Ali"
#with open(output_file, 'a') as file:
 #   for item in output:
  #      file.write(f"{item}\n")

try:
    with open("new.txt", 'r') as file:
            data = [line.split() for line in file.readlines()]
            print(data)
except IOError:
    print("Error: No File")
finally: 
    print("Existing")