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

def find_split(data):
    # We're gonna find the first timestamp where:
    # Channel 1 == 2, Channel 3 < 3 at the same time
    channel_1_values = {}
    channel_3_values = {}

    for row in data:
        timestamp = row['timestamp']
        value = row['value']
        channel = row['channel']

        if channel == 'channel 1':
            channel_1_values[timestamp] = value
        elif channel == 'channel 3':
            channel_3_values[timestamp] = value

    # Lets now find the timestamps that are in both channels
    shared_timestamps = []
    for timestamp in channel_1_values:
        if timestamp in channel_3_values:
            shared_timestamps.append(timestamp)
    
    # Let's go through those timestamps and check condition
    for timestamp in shared_timestamps:
        c1_val = channel_1_values[timestamp]
        c3_val = channel_3_values[timestamp]

        if c1_val == 2 and c3_val < 3:
            # Here we found our split point
            return timestamp
        
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
    
    split = find_split(data)

    if split:
        print(f"Split found at timestamp: {split}")
    else:
        print("Split condition not found here.")

if __name__ == "__main__":
    main()