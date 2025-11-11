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
        channel = row['channel'].lower()

        if channel == 'channel 1':
            channel_1_values[timestamp] = value
        elif channel == 'channel 3':
            channel_3_values[timestamp] = value

    # Lets now find the timestamps that are in both channels
    shared_timestamps = []
    for timestamp in channel_1_values:
        if timestamp in channel_3_values:
            shared_timestamps.append(timestamp)
    
    # sort timestamps for chronological order
    shared_timestamps.sort()

    # Track previous state to detect transitions
    prev_both_met = False
    split_timestamp = []

    # Let's go through those timestamps and check condition
    for timestamp in shared_timestamps:
        c1_val = channel_1_values[timestamp]
        c3_val = channel_3_values[timestamp]

        if c1_val == 2 and c3_val < 3:
            both_met = True
        
        if both_met and not prev_both_met:
            # Transition detected
            split_timestamp.append(timestamp)
            print(f"Split condition met at timestamp: {timestamp}")

        prev_both_met = both_met
    
    return None  # split not found

def write_output(data, split, input_file):
    # Writes the output files based on the split point
    if split is None:
        print("Split not found.")
        return
    
    # creates segments based on split timestamp
    segments = []
    current_segment = []

    for row in data:
        # check if we've hit a split point
        if row['timestamp'] in split and current_segment:
            segments.append(current_segment)
            current_segment = []
        
        current_segment.append(row)

    # add final segment
    if current_segment:
        segments.append(current_segment)

    # write each segment to a file
    base_name = os.path.splitext(os.path.basename(input_file))[0]

    for i, segment in enumerate(segments, start=1):
        output_file = f"{base_name}_part{i}.dat"

        with open(output_file, 'w') as file:
            # write header
            file.write("Timestamp|Value|Channel\n")
            for row in segment:
                file.write(f"{row['timestamp']}|{row['value']}|{row['channel']}\n")
        print(f"Written segment {i} to {output_file} with {len(segment)} rows")
        
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
    
    splits = find_split(data)

    write_output(data, splits, input_file)

if __name__ == "__main__":
    main()