# GDELT Processor

Click command + shift + v to see the preview (it requires the VSC extension "Markdown All in One")

### Table of contents:
- [GDELT Processor](#gdelt-processor)
    - [Table of contents:](#table-of-contents)
- [Prior requirements](#prior-requirements)
- [Main description](#main-description)
    - [Short files explanation](#short-files-explanation)
    - [File requirements to run the code](#file-requirements-to-run-the-code)
- [Code description: Main functions](#code-description-main-functions)
- [Code description: Input file --\> ACTION TO BE TAKEN BY THE USER](#code-description-input-file----action-to-be-taken-by-the-user)

# Prior requirements
As part of the code, a bootstrap method is used to:
1. Create an environment for this processing
2. Install the required packages for the processing

What you should do in the very beginning is run the file [Bootstrap environment](bootstrap_env.py) or input in the console python bootstrap_env.py

After that please activate the conda environment created conda activate GDELT

IMPORTANT: if you are using Visual Studio Code, do not forget to do command (or control for Windows) + shift + P to select your new created GDELT environment as your python interpreter.

# Main description

The code is used to process the GDELT files on the page: http://data.gdeltproject.org/gdeltv2/masterfilelist.txt

The GDELT dataset consists of a set of three files that are uploaded every 15 minutes. These files process news from different sources. A more detail explanation on what is actually being processed can be found here: https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/

### Short files explanation
As above mentioned, there are three files being processed every three minutes on GDELT's site:
- The GDELT Global Knowledge Graph (GKG) V2.1 data is a tab-delimited file (with a ".csv" extension) where each row represents a single document (such as a news article) codified by GDELT’s natural language processing algorithms. The data captures a wide array of metadata about global news, including people, organizations, locations, counts, themes, emotions, dates, images, videos, quotations, and more. Unlike typical CSV files, GKG files do not have headers, so understanding the columns requires reference to the codebook. The data is optimized for fast, parallel processing and is designed for advanced users who may need to preprocess and script the data for analysis GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf-1 .
- The Mentions Table in GDELT 2.0 tracks every mention of an event across the global media system, allowing users to follow the trajectory and network structure of stories as they propagate. Each mention receives its own entry, recording details such as the source, document identifier, sentence and character offsets, whether the event was found in raw text or required advanced NLP, confidence in extraction, document length, and tone. The table enables filtering by confidence and positioning, helping users identify the most prominent or reliable mentions of an event. It supports analysis of event coverage, ambiguity, and translation provenance, making it possible to trace both real-time and historical discussions of events in the media.
- The GDELT Event Table records global events using an expanded dyadic CAMEO format, capturing two actors and the action performed by Actor1 upon Actor2. Each event record includes detailed attributes for both actors (such as geographic, ethnic, religious, and role codes), hierarchical event action codes, a Goldstein ranking score for theoretical impact, and landmark-level georeferencing for actors and actions. The table also provides metrics on the number of mentions, sources, and articles for each event, as well as an average tone score. Events are stored as tab-delimited records, each with a unique identifier and multiple date formats for flexible analysis. The table allows users to analyze events by actor, action, location, and impact, and is designed to facilitate both temporal and spatial queries across the global media system.

### File requirements to run the code
The processing is being done using three main files:
1. A dictionary containing the columns' headers for each of the files (as the files itself from the page do not contain any headers and therefore they have to be mapped).
[Go to Dictionaries file](./Dictionary/Dictionaries.xlsx)
2. A python file with classes and functions (OOP) that helps the user and allows it to process the files (especially the gkg file) focused on the usage of the Themes in the gkg file. It does not only process and enables filtering, but also joins them (using gkg as the main database for the joining) and draws statistics out of it.
[Go to OOP GDELT Processing file](./DataProcessingClasses/OOP_DirectGDELT_Processing.py)
3. A python file in which the classes from the above mentioned document are imported and used for processing of the file. In this file we control the inputs as per our requirements. This enables the user to create a flexible processing of the files that adapts to their own requirements.
[Go to OPP GDELT Input and Process control file](GDELT_Process.py)

# Code description: Main functions

Here the classes included in the files will be explained to understand their role in processing the databases. The file being explained here is the following: [OOP GDELT Processing file](./DataProcessingClasses/OOP_DirectGDELT_Processing.py)

0. GDELTFileSet, GDELTMappingQuality are just dataclasses defined to hold specific input structures to be used in our main classes and their functions.
1. ThemeParser --> This class contains a series of functions that are used for the themes comparison in the gkg file
2. GKGProcessor --> This class contains a series of functions that are used to process the gkg file. This is because the gkg file has themes, and these themes could be used to build join stories of what is included in one document. As the format in which they appear is not easily processable (it includes themes, offset character {where can it be found in the document}), it is being process to get rid of this problem, separating the offset and the themes. On top of this, there are two themes: one from V! and V" of GDELT algorithm. The processor also comapres the differences of themes between rows for a better recognition of what was included in V1 in comparison to V2.
3. KeyColumnsCheckUp --> This class contains a series of functions that are used to check the uniqueness of the keys to map our documents. In the end, this will return a dictionary of files (in case we compare gkg-mentions-export, we will have two files, gkg-mentions, mentions-export, otherwise just one file according to the comparison gkg-export or gkg-mentions, or nothing in case we will just be processing gkg) file which compares, for example if we map export to gkg, it will compare the key column from gkg used for mapping against the key column from export used to mapped the elements to gkg and checked not only which keys matched, but also how many times (as gkg is document driven and export is even driven, and each document contains different events, we want to understand the relationship 1(gkg):howmany(export)).
4. DataJoiner --> This class contains a series of functions that are used to merge a defined
list of columns from mentions and export to gkg however; depending on the joint, the merging is performed.  
    4 cases:
    - gkg only
    - mentions mapped to gkg
    - export mapped to gkg
    - export mapped to mentions and mentions to gkg
Here the set key columns for the mapping of each file (in the code they are not an input but fixed)
gkg: gkg_v2documentidentifier
mentions: mentionidentifier
export: globaleventid
5. MappingAnalyzer --> This class contains a series of functions to get the count of elements from the tones in mentions and export files for which no match in the gkg was found. This is important to get an idea on the actual size of our data set (results are displayed on the terminal/console).
6. GDELTDataLoader --> This class contains maps the dictionaries per each file gkg, mentions, export
to be the new headers per document. It also pulls the files from the website (the ones required. example: only gkg). Please notice that the class asummes:
    - A well structured dictionary filewith sheets names = gkg, mentions, export
    - Access to the correct gdelt site in the function to download data (the site is given inside the function "download_gdelt_files" in the class, it is not an input to control).
7. GDELTProcessor --> This is the main class. It uses the variables from all other classes to process the GDELT Data. It also add extra filters to the data that one can control as inputs.
8. GDELTTimestampBatchRunner --> This is will wrap my GDELTProcessor to process ranges of timestamps. That means, we can define a range of timestamps (the time stamps define the date and time the files where submitted to the GDELT site). The timestamps sytaxis looks this way: YYYYMMDDHHMMSS (Year - Month - Day - Hour - Minutes - Seconds)
The results will also be saves in tow files for all the timestamps (one for statistics - in case it was controlled in the inputs like this - and one for the joint file).

# Code description: Input file --> ACTION TO BE TAKEN BY THE USER

Here it will be explained how to control the inputs in the file [OPP GDELT Input and Process control file](GDELT_Process.py) which is also used to get and save the results from the classes above explained.

The file is divided in:
1. Section where the classes are imported
2. Inputs set up and processing and saving of results by alling the classes above mentioned found under "if _name_ == "_main_":", while this is subdivided into two:
   - INPUTS TIME: Here is where we control the results. The inputs will be explained here:
     1. The ones needed directly as inputs of the GDELTProcessor class
        - dictionary_path: path to the file with the headers of each fileset (is an excel file with 3 sheets, one per file, DO NOT MODIFY THE FILE) --> NOT OPTIONAL
        - output_dir: path to the folder or location where the output files should be stored --> NOT OPTIONAL
        - country_codes: This one will be used to filter out countries in gkg from column "gkg_V2ENHANCEDLOCATIONS" and from Export in "Actor1Geo_CountryCode" --> OPTIONAL
        - themes_tags: Themes in "gkg_V2ENHANCEDTHEMES_list_str" (after gkg has been processed using class GKGProcessor) column that begin with this in the Themes are kept. --> OPTIONAL
        - gkg_columns_to_drop: columns to drop from the gkg to be dropped at the beginning of GKGProcessor. PLEASE DO NOT DROP any themes related columns or the v2documentidentifier (gkg_v2documentidentifier), because they are needed for the GKGProcessor and for the DataJoiner respectively. --> OPTIONAL
        - mentions_columns_to_map: Which columns from the document mentions will be mapped into the gkg --> OPTIONAL
        - export_columns_to_map: Which columns from the document export will be mapped into the gkg --> OPTIONAL
     2. Other inputs that used in GDELTProcessor class but that are not diferect inputs of the main function of the class and which are englobed within a in the beginning defined class "GDELTFileSet"
        - timestamp: this is to set a timestamp with format YYYYMMDDHHMMSS --> OPTIONAL as later we can also define a range of timestamps for GDELTTimestampBatchRunner
        - joincase: # The join is done on gkg, so gkg works as the fixed data frame, whose rows may be multiplied (in case of a 1 to many relationship mapping), but its elements will remain the same.
        Possible values: gkg_only, gkg_mentions, gkg_export, all (gkg <- mentions <- export>) --> NOT OPTIONAL
        - statistics: Here we set if we need want as ouput as: --> NOT OPTIONAL
            a. all: all statistics shown --> returns the joint df (class: DataJoiner), the key column checkup df (class: KeyColumnsCheckUp) and the console statistics from (class: MappingAnalyzer)
            b. key_columns_stats: processor.save_key_columns_analysis should be saved, but not the other stats --> returns the joint df (class: DataJoiner) and the key column checkup df (class: KeyColumnsCheckUp)
            c. none: --> returns the joint df (class: DataJoiner)
        - key_column_dictionary_document: This one is tied to joincase DIRECTLY. Both inputs should be parallel (in a future version this input will be erased as one joincase determines it directly). The possible inputss are: --> NOT OPTIONAL
            a. {}: if join case is equal to gkg_only
            b. {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "mentions": "MentionIdentifier"}: if join case is equal to gkg_mentions
            c. {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "export": "SOURCEURL"}: if join case is equal to gkg_export
            d. {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "mentions": ["MentionIdentifier", "GlobalEventID"], "export": "GlobalEventID"}: if join case is equal to all
     3. Other inputs related to "mapping statistics"that used in GDELTProcessor class but that are not diferect inputs of the main function of the class and which are englobed within a in the beginning defined class "MappingAnalyzer"
        - checkmapping_cols: how much was mapped from a based column with respect to a mapped column <-- OPTIONAL and only REQUIRED if join case was set to all
        - identifier_col: identifier column of the base database to make the comparison <-- OPTIONAL and only REQUIRED if join case was set to all
     4. Inputs that are used in the class GDELTTimestampBatchRunner to wrap up the results for the whole range of time stamps
        - base_fileset: pulls the inputs from the GDELTFileSet above explained <-- NOT OPTIONAL AND ALREADY SET; NOT TO BE CHANGED
        - mapping_columns: pulls the inputs from the GDELTMappingQuality above explained <-- NOT OPTIONAL AND ALREADY SET; NOT TO BE CHANGED
        - timestamp_start: to set the staring timestamp of the range in the format YYYYMMDDHHMMSS. Consider that for MMSS, MM should always be a quarter (00, 15, 30, 45) and second should always remain to be 00. --> NOT OPTIONAL
        - timestamp_start: to set the staring timestamp of the range in the format YYYYMMDDHHMMSS. Consider that for MMSS, MM should always be a quarter (00, 15, 30, 45) and second should always remain to be 00. --> OPTIONAL in case just a timestamp (timestamp_start) is to be processed and not a range.
        - on_error: two possible values "raise" or "skip". If "raise", then when a the code processing a timestamp file set encounters an error it will raise the problem and stop the process at this point. If "skip", then, even if it encounters an error processing a timestamp file set, it will just skip it and continue processing the next one. --> NOT OPTIONAL
        - return_mode: two possible values "always_dict" or "match_processor". If "always_dict" the final results will be displayed in a "dictionary" format on the console. If "match_processor" the console results will be displayed in the same format as processor.process_fileset() --> NOT OPTIONAL
        - flatten_df_key_columns_stats: can be True or False. If True it will save df_key_columns_stats as a single flattened dict (this is good for Excel export): --> NOT OPTIONAL
      5. Other inputs that will be taking in the next step
          - join_df_format: file extension/format of the join df to be saved. Possible values: "csv", "xlsx", "parquet", "pkl" (pickle) --> NOT OPTIONAL
          - key_column_analysis_format: file extension/format of the key column df anaylsis to be saved. Possible values: "csv", "xlsx", "parquet", "pkl" (pickle) --> NOT OPTIONAL
   - SAVING TIME: Not to be touched, unless "return mode" input is changed to "match_processor", and therefore one part of the code needs to be commented out, while the other gets commented in.



