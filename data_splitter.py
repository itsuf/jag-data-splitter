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
                    'channel': channel.strip(),
                    'original_index': len(data)
                })
            except ValueError:
                # If we cant parse value as float, skip this line
                print(f"Warning: Could not parse value on line {line_num}: {value}")
                continue

    return data

def find_split(data):
    # We're gonna find the timestamps where:
    # Channel 1 == 2, Channel 3 < 3 
    timestamps = organise_by_timestamp(data)
    sorted_times = sorted(timestamps.keys())

    prev_both_met = False
    split_indices = []

    # Let's go through those timestamps and check condition
    for timestamp in sorted_times:
        channels = timestamps[timestamp]

        # CHeck if both conditions are met
        has_channel_1 = 'channel 1' in channels
        has_channel_3 = 'channel 3' in channels

        if has_channel_1 and has_channel_3:
            channel_1_value = channels['channel 1']['value']
            channel_3_value = channels['channel 3']['value']

            both_met = (channel_1_value == 2) and (channel_3_value < 3)
        else:
            both_met = False
        
        # only split when we go from not met to met
        if both_met and not prev_both_met:
            # find which row to start the new segment from
            indicies = [channl['index'] for channl in channels.values()]

            split_index = min(indicies)
            split_indices.append(split_index)

            print(f"Split found at timestamp {timestamp} (row index {split_index})")

        prev_both_met = both_met

    return sorted(split_indices)

def create_segments(data, split_indices):
    # Split data into segments based on split indices
    if not split_indices:
        return [data]
    
    segments = []
    start_index = 0

    for split_index in split_indices:
        if split_index > start_index:
            segments.append(data[start_index:split_index])
            start_index = split_index
    
    # add what remains as final segment
    if start_index < len(data):
        segments.append(data[start_index:])

    return segments

def write_output(segments, input_file):
    # Now we gotta write pre-created segments 
    base_name = os.path.splitext(os.path.basename(input_file))[0]

    for i, segment in enumerate(segments, start=1):
        output_file = f"{base_name}_part{i}.dat"

        with open(output_file, 'w') as file:
            # Write header
            file.write("Timestamp|Value|Channel\n")
            
            for row in segment:
                file.write(f"{row['timestamp']}|{row['value']}|{row['channel']}\n")
        
        print(f"Written segment {i} to {output_file} with ({len(segment)} rows)")
        
def organise_by_timestamp(data):
    # Group channel readings by timestamp
    timestamp_dict = {}

    for row in data:
        timestamp = row['timestamp']
        if timestamp not in timestamp_dict:
            timestamp_dict[timestamp] = {}
        
        channel = row['channel'].lower()
        timestamp_dict[timestamp][channel] = {
            'value': row['value'],
            'index': row['original_index']
        }
    
    return timestamp_dict

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
    
    # Find split points
    splits = find_split(data)
    print(f"Found {len(splits)} split points\n")

    # Create segments
    segments = create_segments(data, splits)
    print(f"Created {len(segments)} segments\n")

    # Write output files
    write_output(segments, input_file)
    print("Finished!")

if __name__ == "__main__":
    main()