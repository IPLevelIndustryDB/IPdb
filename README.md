# IPdb
IPdb: A High-precision Industry Categorization of IP Addresses

We provide the IP to Industry dataset based on manual work in ip_industry_2024_01.csv

We also provide a Large Language Model based Industry Categorization System in the folder /LLMICS

To use the system, you should
1. configure the /LLMICS/params.py, to set path or API key of the LLMs
2. run: python ./LLMICS/main.py <input_file> <output_file>

   <input_file> specifies the path to the input csv format file, which contains the organization name and description, each on a separate line.

   <output_file> specifies the path to the output csv format file
