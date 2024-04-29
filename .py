def replace_spaces(input_filename):
    try:
        with open(input_filename, 'r') as file:
            lines = file.readlines()

        modified_lines = []
        if len(lines) > 3:  # Check if there are at least 4 lines in the file
            line_4 = lines[3]
            dollar_indices = [index for index, char in enumerate(line_4) if char == '$']
            for i, line in enumerate(lines):
                modified_line = line
                if i > 3:  # Start replacing spaces in subsequent lines after line 4
                    for index in dollar_indices:
                        if len(line) > index and line[index] == ' ':
                            modified_line = modified_line[:index] + '$' + modified_line[index + 1:]
                modified_lines.append(modified_line)

        with open(input_filename, 'w') as file:
            file.writelines(modified_lines)

        print("Replacement completed successfully. File updated:", input_filename)
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print("An error occurred:", str(e))

input_file = "histo_cotation_2020.txt"  # Replace 'example.txt' with your file name
replace_spaces(input_file)


