import csv

def replace_semicolon_with_comma(file_path):
    try:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)

        for row in rows:
            for i in range(len(row)):
                row[i] = row[i].replace(';', ',')

        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)

        print("Semicolons replaced with commas successfully!")
    except FileNotFoundError:
        print("File not found. Please provide a valid file path.")

if __name__ == "__main__":
    file_path = input("Enter the path of the CSV file: ")
    replace_semicolon_with_comma(file_path)
