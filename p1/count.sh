#!/bin/bash

# Student Name: Badri Narayanan Murali Krishnan
# Student Email: bmuralikrish@wisc.edu
# I worked on this programming assignment alone and did not refer to any material online

# Command to download the zip file
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip

# Performing unzip to extract the contents of the zip file
zcat hdma-wi-2021.zip > hdma-wi-2021.csv

# Counting the lines containing "Multifamily"
count=$(grep -c "Multifamily" hdma-wi-2021.csv)

# Result
echo "Number of lines containing 'Multifamily': $count"

# Deleting the files
rm hdma-wi-2021.zip hdma-wi-2021.csv