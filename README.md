Data Splitter

The script reads a telemtry .dat file and segments into another file when certain conditions are met.

Conditions:

1. Channel 1 value is 2
2. Channel 3 value is less than 3
3. Both must be true at the same timestamp

Here is how is works:

1. Reads the input file (for example, ExampleData.dat)
2. Finds all the points where the conditions are met
3. It splits the data into segments at those points
4. It saves every segment into a new file inside a dedicated folder

Usage:

python3 data_splitter.py ExampleData.dat

Output:

Output is given in a folder named [Original file name]_segments
[Original File Name]_segment_1.dat
[Original File Name]_segment_2.dat
...