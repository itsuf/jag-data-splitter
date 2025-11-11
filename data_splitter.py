import os
import sys


def read_data_file(file_path):
    # Reads and parses the data file
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    data = []
    with open(file_path, 'r') as file:
        # Read header
        header = file.readline().strip()

        for line_num, line in enumerate(file, start=2):
            line = line.strip()
            if not line: # Skips if line is empty
                continue

            parts = line.split('|')
            if len(parts) != 3:
                print(f"Warning: Skipping malformed line {line_num}: {line}")
                continue
            
            timestamp, value, channel = parts

            try:
                data.append({
                    'timestamp': timestamp,
                    'value': float(value), # float vals exist so we convert
                    'channel': channel.strip()
                })
            except ValueError:
                # If we cant parse value as float, skip this line
                print(f"Warning: Could not parse value on line {line_num}: {value}")
                continue

    return data

def main():
    if len(sys.argv) != 2:
        print("Usage: python data_splitter.py <input_file>")
        return
    
    input_file = sys.argv[1]

    print(f"Reading file: {input_file}")

    try:
        data = read_data_file(input_file)
        print(f"Read {len(data)} rows")
    except Exception as e:
        print(f"Error: {e}")
        return

if __name__ == "__main__":
    main()