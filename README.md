# EJScreen Traffic Proximity Indicator Processing Summary

This summary describes the process for taking highway segments with traffic counts and producing Census Block Group-level proximity scores. The workflow is described in detail, and where appropriate, sample scripts are referenced. The bulk of the work process is manual, but sample scripts are provided as a guide through the process. From start to finish, this a state-by-state process. The is provided provides an overview of all components of the HPMS traffic proximity process for EJScreen. This includes the input sources, output results, software and platform pieces, and the overall workflow. Note that Hadoop processing is performed all at once by creating an EMR cluster with job steps define for each individual state. In more general terms, work was broken into 3 subtasks—Create highway segments with traffic counts for input (Pre-Hadoop); Generate Census block-level proximity scores and aggregate to Block Group-level scores (AWS Hadoop Processing); and Download results, merge states to national file and combine with Block Group geographies for use in EJScreen (Post-Hadoop Processing).

**Sources:**

DOT Federal Highway Administration Highway Performance Monitoring System (HPMS). HPMS is a national level highway information system that includes data on the extent, condition, performance, use and operating characteristics of the nation's highways. This data includes Annual Average Daily Traffic counts (AADT). EJScreen 2.2 uses the 2020 spatial data available at this DOT hosted services ArcGIS server: [https://geo.dot.gov/server/rest/services/Hosted](https://geo.dot.gov/server/rest/services/Hosted).

During the data review process, we noticed a significant number of redundant and overlapping street segments. The necessary additional processing is described in Pre-Hadoop Processing section.

**Pre-Hadoop Processing:**

- Import all 52 feature classes from /hosted/HPMS\_Full\_AL\_2020 … HPMS\_Full\_WY\_2020 into HPMS2020\_Work.gdb AL, …, WY (in environment dialog set M to false).
- Create subset of "Major" highway segments using functional class (f\_system in (1, 2, 3) or (f\_system = 4 and urban\_code \<\> 99999)  HPMS2020\_Major\_AL, … HPMS2020\_Major\_WY
- Remove records where AADT is NULL or AADT = 0 or Shape\_length = 0
- Dissolve redundant and partial overlapping segments
- Add new column: REPEAT\_TEST
- Calculate: REPEAT\_TEST = Route\_ID & AADT
- Run Dissolve tool (dissolve by REPEAT\_TEST; first­\_state\_code, first\_urban\_code, mean\_AADT), "No Multipart", "No Unsplit"  HPMS2020\_1\_clean, … HPMS2020\_72\_clean. Note switching StateAbb to StateFIPS here for state identifier. Note that state abbreviations are changed to FIPS codes for each file (for example, AL to 1, WY to 56, PR to 72)
- Add ID (long int, calculated from OBJECTID)
- Use "export to geodatabase" tool to convert Polyline M to Polyline features and alter table structure ready for JSON export, including renaming fields: state\_code, f\_system, urban\_code, aadt, ID  HPMS2020\_1\_forJSON, … HPMS2020\_72\_forJSON
- Export to JSON using ESRI Hadoop tool  ../InputforHadoop/HPMS2020\_1.json, …, HPMS2020\_72.json. Note that toolbox is in ArcGIS/geoprocessing-tools-for-hadoop-master/HadoopTools.py (use Features to JSON with defaults)
- Resulting JSON fie elements should be state\_code, f\_system, urban\_code, aadt, ID, geometry (z and m both false)
- See sample arcpy code for processing 1 state in **misc\_hpms\_process\_steps\_1\_python.txt**.
- Upload each to s3, each in its own folder. For example: s3://ejscreen2023/TrafficProximity/HPMS2020\_1/HPMS2020\_1.json

**Input Datasets:**

- 52 state-level JSON files resulting from the Pre-Hadoop Processing.
- 2020 Census 100% count-based Population-weighted blocks table. It contains the block point locations, block codes, block populations, block group codes, and block weights for population distribution--s3://ejscreen2023/proximity/EJScreen\_BlockWeights\_forHadoop\_noZeroPops\_2KBuf.csv.gz

**AWS Hadoop Processing:**

- Setup AWS EMR cluster.
- Step 0: Convert each state JSON file (with HPMS shapes) to Hive data structures. For example: s3://ejscreen2023/TrafficProximity/HPMS2020\_1  s3://ejscreen2023/TrafficProximity/HPMS2020\_1\_hive/. See **TrafficProxJsontoHiveScript\_1.txt** for sample Hive code.
- Run step 1 for each State. See **Highway\_processing \_1\_step1\_pig.txt** for Pig script example. Note that for each State, the block pop weight table includes a selection of neighboring States.
- Run step 2 for each State. See **Highway\_processing \_1\_step2\_pig.txt** for Pig script example.
- Use Athena to create BG-level results tables and export BG\_Scores\_01.csv, etc. to BG\_Scores\_Hadoop\_Output folder.

**Post-Hadoop Processing:**

- Combine all text. Copy bgscores\*.csv  US\_bg\_scores.csv.
- Prep with text editor (Capitalized first header row and remove all other header rows).
- Convert US\_bg\_scores.csv to xlsx files import from text source and make sure "BLKGRP" column is text) – 295,809 records processed.
- Import Excel files to file geodatabase (TrafficProximity\_Work.gdb).
- By aggregating state by state, some BGs are processed in more than one State. The Frequency tool is BLKGRP with sum(BLKGRP\_SCORE)US\_BG\_Scores\_Final -- 239,921 total records.
- Rename columns to STCNTRBG and BG\_SCORE.
- Provide datasets for testing--Create TrafficProximity\_Testing.gdb.
- Include BG\_Scores\_ALL and BG\_Scores\_Final
- Add feature class TrafficProx\_BG\_2021 with BG 2021 shapes

**EPA Disclaimer**

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. EPA has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA. The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
