# IPdb
[[WWW'25] IPdb: A High-precision Industry Categorization of IP Addresses](https://dl.acm.org/doi/10.1145/3696410.3714669)

**The IPdb dataset is in [Release](https://github.com/IPLevelIndustryDB/IPdb/releases)**

## IPdb system

We also provide the source code of IPdb system

To use the system, you should
1. configure the params.py, to set path or API key of the LLMs, and the config of the database
   
2. get domain:

   - if you have ip to domain mapping files (e.g. ptr files), you can run: 

      ```
      python ./main.py --time_stamp <time_stamp> --mode ip_to_domain --ptr_file <path_to_ptr_file> --pyasn_file <path_to_pyasn_file>
      ```

       to store the ip to domain mapping in the database

   - or if you would like to start with IP addresses, you can run: 

      ```
      python ./main.py --time_stamp <time_stamp> --mode get_ptr --hitlist <path_to_ip_addresses_file> --pyasn_file <path_to_pyasn_file>
      ```

       to get ptr records of the IP addresses and store them in the database

3. run 

   ```
   python ./main.py --time_stamp <time_stamp> --mode whois
   ```

   ```
   python ./main.py --time_stamp <time_stamp> --mode cert
   ```

    to get organization information from whois and certificate information for the IP addresses in the database

4. run 

   ```
   python ./main.py --time_stamp <time_stamp> --mode get_wiki_info
   ```

    to get the wiki information for the organizations

5. run 

   ```
   python ./main.py --time_stamp <time_stamp> --mode label_wiki --label_wiki_model <LLM_you_want_to_use>
   ```

    to categorize the organizations into industries by individual LLMs

6. run

   ```
   python ./main.py --time_stamp <time_stamp> --mode label_wiki --label_wiki_model combine --combine_label_model ./llm_label/llmics.pkl
   ```

    to combine the results of individual LLMs

The results will be stored in the database.


## Labeled Org texts
We provide a gold-standard dataset of manually labeled organizational texts, located at ./org_text_to_industry_anon.csv, for training the combination layer and for evaluation.