import sys


def read_data_file(file_path):
    data = []
    with open(file_path, 'r') as file:
        # Read header
        header = file.readline().strip()

        for line_num, line in enumerate(file, start=2):
            line = line.strip()
            if not line: # Skips if line is empty
                continue

            parts = line.split('|')
            
            timestamp, value, channel = parts

            data.append({
                'timestamp': timestamp,
                'value': value,
                'channel': channel
            })
    return data

def main():
    if len(sys.argv) != 2:
        print("Usage: python data_splitter.py <input_file>")
        return
    
    input_file = sys.argv[1]

    print(f"Reading file: {input_file}")

    data = read_data_file(input_file)
    print(f"Read {len(data)} rows")

if __name__ == "__main__":
    main()