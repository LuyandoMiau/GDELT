"""
MAIN PROCESSING SCRIPT FOR GDELT FILES
==========================================
In this file all the classed needed to process the GDELT files are called
On top of this, the inputs required are given and managed
"""
 
# Import the classes with all its functions
from DataProcessingClasses.OOP_DirectGDELT_Processing import (
    GDELTFileSet,
    GDELTMappingQuality,
    ThemeParser,
    GKGProcessor,
    KeyColumnsCheckUp,
    DataJoiner,
    MappingAnalyzer,
    GDELTDataLoader,
    GDELTProcessor,
    GDELTTimestampBatchRunner
)

import os
 
# ================== Resolve paths relative to this script ======================
 
# Path of this file: .../dataProcessing/GDELT_Process.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # This is the cd where I am running the code
 
# Path to dictionary: BASE, folder, file
DICT_PATH = os.path.join(BASE_DIR, "Important_documents", "Dictionaries.xlsx") # <--- EDIT IF NEEDED
DICT_PATH = os.path.abspath(DICT_PATH)
 
# Path to output directory: BASE, file
OUTPUT_DIR = os.path.join(BASE_DIR, "Output") # <--- EDIT IF NEEDED
OUTPUT_DIR = os.path.abspath(OUTPUT_DIR)
 
# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)
 
# Process ------------------------------------------------------------- DOWN --------------->
 
if __name__ == "__main__":
 
    # ======================== INPUTS TIME ==================================================
 
    # 1 ---> __init__ function --> arguments of the GDELTProcessor
 
    # Here we need arguments: dictionary_path, output_dir, and optional column specifications
    processor = GDELTProcessor(
        # dictionary_path="./Dictionary/Dictionaries.xlsx",
        # output_dir="./Output",
        dictionary_path=DICT_PATH,
        output_dir=OUTPUT_DIR,
        country_codes=[ # Optional
        'FR', 'GM', 'LU', 'LH', 'PL', 'RE', 'AU', 'BE', 'SP', 'DA', 'SZ', 'NL', 'FI', 
        'IT', 'HU', 'BU', 'SW', 'LG', 'EZ', 'MT', 'EN', 'IC', 'GL', 'LS', 'LO', 'SI', 
        'CY', 'NO', 'PO', 'HR', 'VT', 'MN', 'GK'
    ],
        themes_tags=["EPU", "TAX"], # Optional: Themes in "gkg_V2ENHANCEDTHEMES_list_str" column that begin with this in the Themes
        gkg_columns_to_drop=[ # Optional
        "V2SOURCECOLLECTIONIDENTIFIER", "V2GCAM", "V2.1SHARINGIMAGE",
        "V2.1RELATEDIMAGES", "V2.1SOCIALIMAGEEMBEDS", "V2.1SOCIALVIDEOEMBEDS",
        "V2.1QUOTATIONS", "V2.1ALLNAMES", "V2.1AMOUNTS", "V2.1ENHANCEDDATES",
        "V2.1TRANSLATIONINFO", "V2EXTRASXML", "V1COUNTS", "V2.1COUNTS",
        "V1PERSONS", "V2ENHANCEDPERSONS", "V1ORGANIZATIONS", "V2ENHANCEDORGANIZATIONS"
    ],
        mentions_columns_to_map=["MentionDocTone"], # Optional
        export_columns_to_map=[ # Optional
            "Actor1Code",
            "Actor1Name",
            "Actor1Geo_Type",
            "Actor1Geo_Fullname",
            "Actor1Geo_CountryCode",
            "NumMentions",
            "GoldsteinScale",
            "AvgTone"]
    )
   
    # 2 ---> process_fileset function --> inputs used inside the GDELTProcessor
 
    # Here we define the fileset with timestamp, joincase, statistics level, and key column dictionary
    # The key_column_dictionary_document structure adapts to the joincase
    base_fileset = GDELTFileSet(
        timestamp="20251201143000", # This will be IGNORED if timestamp_start is provided in runner.run()
        # The join is done on gkg, so gkg works as the fixed data frame
        # Whose rows may be multiplied (in case of a 1 to many relationship mapping)
        # But its elements will remain the same
        joincase="gkg_export", # Possible values: gkg_only, gkg_mentions, gkg_export, all
        # all: all statistics shown
        # key_columns_stats: processor.save_key_columns_analysis should be saved, but not the other stats
        # none: no stats performed, just save the joint file
        statistics="key_columns_stats", # Possible values: all, key_columns_stats, none
        # Key column dictionary adapts to joincase:
        # For "gkg_only": None or {}
        # For "gkg_mentions": {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "mentions": "MentionIdentifier"}
        # For "gkg_export": {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "export": "SOURCEURL"}
        # For "all": {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "mentions": ["MentionIdentifier", "GlobalEventID"], "export": "GlobalEventID"}
        key_column_dictionary_document={"gkg": "gkg_V2DOCUMENTIDENTIFIER", "export": "SOURCEURL"}
    )
 
    # Here we need 2 arguments: checkmapping_cols, identifier_col (only needed if statistics == 'all')
    # Since we're using statistics="key_columns_stats", mapping_columns is optional but we define it anyway
    mapping_columns = GDELTMappingQuality(
        checkmapping_cols=["gkg_ACTUAL_TONE", "Export_AvgTone"], # How much of tones was mapped and how much was not mapped
        identifier_col="gkg_V2DOCUMENTIDENTIFIER"
    )
   
    # 3 ---> Run GDELTTimestampBatchRunner
 
    # THIS LINE HERE BELOW IS NOT TO BE CHANGED ----------------------------------
    runner = GDELTTimestampBatchRunner(processor) # we call our inputs in processor
    # -----------------------------------------------------------------------------
 
    # WE COME BACK TO INPUTS NOW
    batch_result = runner.run( # we call the function run using runner as inputs for __init__ in GDELTTimestampBatchRunner
        base_fileset=base_fileset,
        mapping_columns=mapping_columns, # Can be None if statistics="none" or "key_columns_stats"
        timestamp_start="20251201143000", # 01.Dec.2025, 14:30:00
        timestamp_end="20251202143000", # 02.Dec.2025, 14:30:00 (this will process 97 timestamps: 24hrs * 4 per hour + 1)
        on_error="raise", # It could also be "skip" to continue processing even if some timestamps fail
        return_mode="always_dict", # Otherwise "match_processor" to return in the same format as processor.process_fileset()
        flatten_df_key_columns_stats=True # To save df_key_columns_stats as a single flattened dict (this is good for Excel export)
    )

    join_df_format = "xlsx" # Can be "csv", "xlsx", "parquet", "pkl" (pickle)
    key_column_analysis_format = "xlsx" # Can be "csv", "xlsx", "parquet", "pkl" (pickle)
 
    # ======================== SAVING TIME ==================================================
 
    # WHEN RETURN MODE WAS "always_dict" =======> comment or outcomment the block according to the inputs above
 
    # Extract timestamps for filename
    timestamp_range = f"{batch_result['timestamps_requested'][0]}-{batch_result['timestamps_requested'][-1]}"
    
    # Save the joined df (this is always present regardless of statistics level)
    processor.save_results(
        batch_result["joined_df"], 
        timestamp_range, 
        format=join_df_format
    )
 
    # Save key-column checkup workbook (only if base_fileset.statistics="key_columns_stats" or "all")
    # Check if we have the flattened stats (when flatten_df_key_columns_stats=True)
    if "stats" in batch_result and batch_result["stats"]:
        
        # If flatten_df_key_columns_stats=True, use the flattened version
        if "df_key_columns_stats_flat" in batch_result["stats"]:
            flat_stats = batch_result["stats"]["df_key_columns_stats_flat"]
            
            # Only save if there's actually data
            if flat_stats:
                processor.save_key_columns_analysis(
                    flat_stats, 
                    timestamp_range, 
                    format=key_column_analysis_format
                )
            else:
                print("No key column statistics to save (empty result)")
        
        # If statistics="all", we also have other stats available
        if base_fileset.statistics == "all":
            
            # Print console statistics for each timestamp
            print("\n" + "="*80)
            print("KEY COLUMN MAPPING STATISTICS BY TIMESTAMP")
            print("="*80)
            
            key_col_stats_by_ts = batch_result["stats"].get("key_columns_stats_by_timestamp", {})
            for ts, stats in key_col_stats_by_ts.items():
                print(f"\nTimestamp: {ts}")
                for file_key, metrics in stats.items():
                    print(f"  {file_key.upper()} File:")
                    for metric_key, metric_value in metrics.items():
                        print(f"    {metric_key}: {metric_value}")
            
            # Print mapping quality statistics
            print("\n" + "="*80)
            print("MAPPING QUALITY STATISTICS BY TIMESTAMP")
            print("="*80)
            
            mapping_stats_by_ts = batch_result["stats"].get("mapping_stats_by_timestamp", {})
            for ts, stats in mapping_stats_by_ts.items():
                print(f"\nTimestamp: {ts}")
                for col, metrics in stats.items():
                    print(f"  {col}: {metrics}")
    
    # Print summary of batch processing that was saved within the dictionary
    print("\n" + "="*80)
    print("BATCH PROCESSING SUMMARY")
    print("="*80)
    print(f"Timestamps requested: {len(batch_result['timestamps_requested'])}")
    print(f"Timestamps processed: {len(batch_result['timestamps_processed'])}")
    print(f"Timestamps failed: {len(batch_result['timestamps_failed'])}")
    
    # In case there were any timestamps that were not processed, please print
    if batch_result['timestamps_failed']:
        print("\nFailed timestamps:")
        # For each timestamp print the error message
        for ts, error_msg in batch_result['timestamps_failed'].items():
            print(f"  {ts}: {error_msg}")
    
    # As summary please print the size of the joined df
    print(f"\nFinal joined dataframe shape: {batch_result['joined_df'].shape}")
    print("="*80)
    
    # WHEN RETURN MODE WAS "match_processor" =======> comment or outcomment the block according to the inputs above
    
    # For statistics="none":
    # result_df = batch_result
    # processor.save_results(result_df, timestamp_range, format=join_df_format)
    
    # For statistics="key_columns_stats":
    # df_key_col_stats, result_df = batch_result
    # processor.save_results(result_df, timestamp_range, format=join_df_format)
    # if df_key_col_stats:
    #     processor.save_key_columns_analysis(df_key_col_stats, timestamp_range, format=key_column_analysis_format)
    
    # For statistics="all":
    # key_col_stats, df_key_col_stats, result_df, mapping_stats = batch_result
    # processor.save_results(result_df, timestamp_range, format=join_df_format)
    # if df_key_col_stats:
    #     processor.save_key_columns_analysis(df_key_col_stats, timestamp_range, format=key_column_analysis_format)
    # # Then you'd need to manually format and print key_col_stats and mapping_stats
