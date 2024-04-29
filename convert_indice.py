

import csv

def convert_to_csv(input_filename, output_filename, column_names):
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

        # Write modified content to CSV file
        with open(output_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)
            for line in modified_lines:
                row = line.strip().split('$')
                writer.writerow(row)

        print("Conversion to CSV completed successfully. New file created:", output_filename)
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print("An error occurred:", str(e))
import csv

# Define the column names
column_names = ["SEANCE", "CODE_INDICE", "LIB_INDICE", "INDICE_JOUR", "INDICE_VEILLE", "VARIATION_VEILLE", "INDICE_PLUS_HAUT", "INDICE_PLUS_BAS", "INDICE_OUV"]


input_file = "histo_indice_2021.txt"  # Replace 'input.txt' with your input file name
output_file = "histo_indice_2021.csv"  # Replace 'output.csv' with the desired output CSV file name
convert_to_csv(input_file, output_file, column_names)

# Spécifiez les noms de colonnes appropriés

