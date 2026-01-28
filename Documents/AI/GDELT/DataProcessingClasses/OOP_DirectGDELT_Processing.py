""" THIS ONE PULLS A SET (only one key: yearmonthdayhourminutesseconds) OF GDELT FILES DIRECTLY FROM GDELT SITE"""

import re
import time
import csv
import sys
import os
import pickle
import pandas as pd
import numpy as np
import requests
import zipfile
import io
import sqlite3
from typing import Dict, List, Tuple, Any, Optional, Union, Literal
from pathlib import Path
from datetime import datetime
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

""" These are data classes that will be input for our functions"""

# This one is for the time stamp and the files' paths
@dataclass
class GDELTFileSet:
    """
    Represents a set of GDELT files from a 15-minute interval.
    
    Attributes:
        timestamp: Timestamp in format YYYYMMDDHHMMSS
        joincase: Type of join to perform ("gkg_only", "gkg_mentions", "gkg_export", "all")
        statistics: Level of statistics to generate ("all", "key_columns_stats", "none")
        key_column_dictionary_document: Dictionary mapping file types to their key columns
    """
    timestamp: str
    joincase: str
    statistics: str
    key_column_dictionary_document: Optional[Dict[str, Union[str, List[str]]]] = None

# This one is for the columns for which we want to check how well the mapping between file was done
@dataclass
class GDELTMappingQuality:
    """
    Represents columns to check for mapping quality analysis.
    
    Attributes:
        checkmapping_cols: List of column names to check for empty/unmapped values
        identifier_col: Column name used as the identifier for checking mappings
    """
    checkmapping_cols: list[str]
    identifier_col: str

"""
FIRST CLASS: ThemeParser
NOTICE: THIS CLASS CONTAINS A SERIES OF FUNCTIONS THAT ARE USED FOR THE THEMES COMPARISON IN THE GKG FILE
"""
class ThemeParser:

    """ ONLY FOR GKG THEMES!! """
    
    """This one can handle the gkg Theme columns"""
    
    # It is a static method because does not receive an implicit first argument
    # It is just inside this class for organization purposes
    @staticmethod
    def parse_theme_cell(cell: Any) -> List[Dict[str, Any]]:
        
        """
        Parse a single cell like 'EPU_ECONOMY_HISTORIC,582;EPU_ECONOMY_HISTORIC,827'
        into a list of {'Theme': <str>, 'Number': <int/float/str/None>} dicts.
        """

        if pd.isna(cell):
            return []
        
        text = str(cell).strip()
        if not text:
            return []
        
        items = []
        for token in text.split(';'):
            token = token.strip()
            if not token:
                continue
            
            parts = [p.strip() for p in token.split(',')]
            theme = parts[0] if parts else None
            number = None
            
            if len(parts) >= 2 and parts[1] != "":
                raw_num = parts[1]
                try:
                    number = int(raw_num)
                except ValueError:
                    try:
                        number = float(raw_num)
                    except ValueError:
                        number = raw_num
            
            if theme:
                items.append({'Theme': theme, 'Number': number})
        
        return items
    
    # It is a static method because does not receive an implicit first argument
    # It is just inside this class for organization purposes
    @staticmethod
    def build_theme_dict(df: pd.DataFrame, column: str) -> Dict[Tuple[Any, Any], List[Dict[str, Any]]]:

        """
        Build a dictionary for the given column:
          {(GKGRECORDID, V2DOCUMENTIDENTIFIER): [{'Theme': ..., 'Number': ...}, ...]}
        """

        # Validate existence of the columns
        required = ['GKGRECORDID', 'V2DOCUMENTIDENTIFIER', column]
        for c in required:
            if c not in df.columns:
                raise KeyError(f"Required column missing: {c}")
        
        # Save the dictionary values per key pair
        result = {}
        for _, row in df[required].iterrows():
            key = (row['GKGRECORDID'], row['V2DOCUMENTIDENTIFIER'])
            items = ThemeParser.parse_theme_cell(row[column])
            result[key] = items
        return result
    
    # It is a static method because does not receive an implicit first argument
    # It is just inside this class for organization purposes
    @staticmethod
    def compare_per_key(
        v1_dict: Dict[Tuple[Any, Any], List[Dict[str, Any]]],
        v2_dict: Dict[Tuple[Any, Any], List[Dict[str, Any]]]
    ) -> Dict[Tuple[Any, Any], Dict[str, List[str]]]:

        """Compare themes between V1 and V2 on a per-row basis."""
        # All keys are both the set of keys for each version
        all_keys = set(v1_dict.keys()) | set(v2_dict.keys())
        out = {}
        
        # Per key we will get the union and the differences
        for key in all_keys:
            s1 = set(it['Theme'] for it in v1_dict.get(key, []))
            s2 = set(it['Theme'] for it in v2_dict.get(key, []))
            out[key] = {
                'common': sorted(s1 & s2),
                'only_in_V1THEMES': sorted(s1 - s2),
                'only_in_V2ENHANCEDTHEMES': sorted(s2 - s1),
            }
        return out

"""
SECOND CLASS: GKGProcessor
NOTICE: THIS CLASS CONTAINS A SERIES OF FUNCTIONS THAT ARE USED TO PROCESS THE GKG FILE
"""
class GKGProcessor:

    """Processes GKG files"""
    
    # Default columns to drop, but this can be changed if needed
    """ DEFAULT_COLUMNS_TO_DROP = [
        "V2SOURCECOLLECTIONIDENTIFIER", "V2GCAM", "V2.1SHARINGIMAGE",
        "V2.1RELATEDIMAGES", "V2.1SOCIALIMAGEEMBEDS", "V2.1SOCIALVIDEOEMBEDS",
        "V2.1QUOTATIONS", "V2.1ALLNAMES", "V2.1AMOUNTS", "V2.1ENHANCEDDATES",
        "V2.1TRANSLATIONINFO", "V2EXTRASXML", "V1COUNTS", "V2.1COUNTS",
        "V1PERSONS", "V2ENHANCEDPERSONS", "V1ORGANIZATIONS", "V2ENHANCEDORGANIZATIONS"
    ] """
    
    def __init__(self, columns_to_drop: Optional[List[str]] = None):
        # Call the columns to be dropped
        self.columns_to_drop = columns_to_drop if columns_to_drop is not None else []
        self.logger = logging.getLogger(self.__class__.__name__)
        # Call the class theme parser above defined to use its functions
        self.theme_parser = ThemeParser()
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:

        """Process GKG dataframe with all its transformations"""

        # O Step: create a copy of the df
        df = df.copy()
        # 1st Step: Extract actual tone
        df = self._extract_actual_tone(df)
        # 2nd Step: Process themes
        df = self._process_themes(df)
        # 3rd Step: Drop unnecessary columns
        df = self._drop_columns(df)
        # 4th Step: Add prefix to identify the gkg columns before merging
        df = df.add_prefix("gkg_")
        
        # Display what was processed and return the data frame
        self.logger.info(f"Processed GKG data: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    # 1st Step Function --->
    def _extract_actual_tone(self, df: pd.DataFrame) -> pd.DataFrame:

        """Extract the primary tone value from V1.5TONE column"""

        df['ACTUAL_TONE'] = (
            df['V1.5TONE']
            .str.split(',')
            .str[0]
            .str.strip()
        )
        return df
    
    # 2nd Step Function --->
    def _process_themes(self, df: pd.DataFrame) -> pd.DataFrame:

        """Process theme columns and create comparison metrics"""

        # Validate the exitence of the required columns (the themes)
        required = ["V1THEMES", "V2ENHANCEDTHEMES"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Build theme dictionaries using the function "build_theme_dict" inside the class "theme_parser"
        v1_dict = self.theme_parser.build_theme_dict(df, 'V1THEMES')
        v2_dict = self.theme_parser.build_theme_dict(df, 'V2ENHANCEDTHEMES')
        
        # Extract themes and numbers as lists and save them as columns in the data frame
        df['V1THEMES_list'] = df['V1THEMES'].apply(
            lambda s: [it['Theme'] for it in self.theme_parser.parse_theme_cell(s)]
        )
        df['V1NUMBERS_list'] = df['V1THEMES'].apply(
            lambda s: [it['Number'] for it in self.theme_parser.parse_theme_cell(s) if it['Number'] is not None]
        )
        df['V2ENHANCEDTHEMES_list'] = df['V2ENHANCEDTHEMES'].apply(
            lambda s: [it['Theme'] for it in self.theme_parser.parse_theme_cell(s)]
        )
        df['V2NUMBERS_list'] = df['V2ENHANCEDTHEMES'].apply(
            lambda s: [it['Number'] for it in self.theme_parser.parse_theme_cell(s) if it['Number'] is not None]
        )
        
        # Convert to readable strings and save them as columns in the data frame
        df['V1THEMES_list_str'] = df['V1THEMES_list'].apply(lambda lst: ", ".join(map(str, lst)))
        df['V1NUMBERS_list_str'] = df['V1NUMBERS_list'].apply(lambda lst: ", ".join(map(str, lst)))
        df['V2ENHANCEDTHEMES_list_str'] = df['V2ENHANCEDTHEMES_list'].apply(lambda lst: ", ".join(map(str, lst)))
        df['V2NUMBERS_list_str'] = df['V2NUMBERS_list'].apply(lambda lst: ", ".join(map(str, lst)))
        
        # Create row keys and comparisons calling teh function "compare_per_key" inside the class "theme_parser"
        df['RowKey'] = list(zip(df['GKGRECORDID'], df['V2DOCUMENTIDENTIFIER']))
        per_key_comparison = self.theme_parser.compare_per_key(v1_dict, v2_dict)
        
        # Save the differences as columns into our data frame
        df['Theme_row_common'] = df['RowKey'].map(lambda k: per_key_comparison.get(k, {}).get('common', []))
        df['Theme_row_only_in_V1'] = df['RowKey'].map(lambda k: per_key_comparison.get(k, {}).get('only_in_V1THEMES', []))
        df['Theme_row_only_in_V2'] = df['RowKey'].map(lambda k: per_key_comparison.get(k, {}).get('only_in_V2ENHANCEDTHEMES', []))
        
        # Convert comparison results to strings and save them as columns in the data frame
        df['Theme_row_common_str'] = df['Theme_row_common'].apply(lambda lst: ", ".join(map(str, lst)))
        df['Theme_row_only_in_V1_str'] = df['Theme_row_only_in_V1'].apply(lambda lst: ", ".join(map(str, lst)))
        df['Theme_row_only_in_V2_str'] = df['Theme_row_only_in_V2'].apply(lambda lst: ", ".join(map(str, lst)))
        
        # Drop intermediate columns that we do not need at all anymore
        cols_to_drop = [
            'V1THEMES_list', 'V1NUMBERS_list', 'V2ENHANCEDTHEMES_list',
            'V2NUMBERS_list', 'RowKey', 'Theme_row_common',
            'Theme_row_only_in_V1', 'Theme_row_only_in_V2'
        ]
        df.drop(columns=cols_to_drop, inplace=True)
        
        return df
    
    # 3rd Step Function --->
    def _drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        """Drop specified columns from dataframe"""

        # Check if there are any columns in our list of columns that are not in our data frame
        valid_columns = [col for col in self.columns_to_drop if col in df.columns]
        missing_columns = [col for col in self.columns_to_drop if col not in df.columns]
        
        # In case some of our input missing columns are not there, please displayed which were not to be found inside annerror message
        if missing_columns:
            self.logger.warning(f"Columns not found (skipped): {missing_columns}")
        
        # Drop the columns which are actually found inside our data farme
        if valid_columns:
            df.drop(columns=valid_columns, inplace=True)
        
        return df

"""
THIRD CLASS: KeyColumnsCheckUp
NOTICE: THIS CLASS CONTAINS A SERIES OF FUNCTIONS THAT ARE USED TO CHECK THE UNIQUENESS OF THE KEYS TO MAP OUR DOCUMENTS
AND THEIR RELATIONSHIP 1:HOWMANY IF THE ARE NOT UNIQUE
"""

class KeyColumnsCheckUp:

    """ Here we check the key columns of each document to check for compatibility of mapping """

    # POSSIBLE INPUT --->
    # key_column_dictionary_document can take different forms depending on the joincase:
    # - gkg_only: None or empty dict (no checks needed)
    # - gkg_mentions: {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "mentions": "MentionIdentifier"}
    # - gkg_export: {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "export": "GlobalEventID"}
    # - all: {"gkg": "gkg_V2DOCUMENTIDENTIFIER", "mentions": ["MentionIdentifier", "GlobalEventID"], "export": "GlobalEventID"}

    def __init__(self, key_column_dictionary_document: Optional[Dict[str, Union[str, List[str]]]] = None):
        self.key_column_dictionary_document = key_column_dictionary_document or {}
        self.logger = logging.getLogger(self.__class__.__name__)

    """ This one is to check differences in the values of each columns respectively to each unique values, to if the columns contain only unique values or not """

    def check_key_columns(
        self,
        gkg_df: Optional[pd.DataFrame] = None,
        mentions_df: Optional[pd.DataFrame] = None,
        export_df: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        
        """
        Check key columns for uniqueness and relationships.
        
        Inputs:
            gkg_df: GKG dataframe (optional depending on joincase)
            mentions_df: Mentions dataframe (optional depending on joincase)
            export_df: Export dataframe (optional depending on joincase)
        
        Returns:
            Dictionary with statistics about key columns (length, unique values, differences)
        """

        # If no dictionary is provided, return empty results
        if not self.key_column_dictionary_document:
            self.logger.info("No key column dictionary provided, skipping checks")
            return {}

        # Map each dictionary key to its corresponding DataFrame
        df_by_key = {
            "gkg": gkg_df,
            "mentions": mentions_df,
            "export": export_df
        }

        # Dictionary to save the key columns as per the keys in our key_column_dictionary_document
        # This will store a key and 5 assigned values
        # 1. The column itself
        # 2. The amount of items in 1
        # 3. The unique values in 1
        # 4. The amount of items in 3
        # 5. Difference between 2 and 4
        key_mapping_column: Dict[str, Dict[str, Any]] = {key: {} for key in self.key_column_dictionary_document.keys()}

        # Define dictionary for results
        results = {}

        # Get all the values inside the dictionary
        for key in self.key_column_dictionary_document.keys():

            # Get the values per key and save them as an object this will be either a string or a list of strings
            key_dictionary = self.key_column_dictionary_document.get(key)

            # Refer also to the specific data frame
            df_name = df_by_key[key]  # from here we wil get gkg_df, mentions_df and export_df
            
            # Skip if dataframe is None (not provided for this joincase)
            if df_name is None:
                self.logger.warning(f"Dataframe for '{key}' is None, skipping checks for this file")
                continue

            # Normalize to a list of column names, make everything a list even if it is just one string value
            if isinstance(key_dictionary, str):
                key_list = [key_dictionary]
            elif isinstance(key_dictionary, list):
                key_list = key_dictionary
            else:
                raise TypeError(
                    f"For '{key}', expected a string or list of strings; got {type(key_dictionary).__name__}"
                )

            # Validate inputs the column given exists in the df
            valid_columns = [col for col in key_list if col in df_name.columns]
            invalid_columns = [col for col in key_list if col not in df_name.columns]

            # If they are not the same then raise an error
            if invalid_columns:
                raise ValueError(
                    f"Some of the columns you provided to filter the dataframe were not found. "
                    f"Missing columns: {invalid_columns}. The process will not continue..."
                )

            # Now we need to differentiate because mentions have a list of two keys
            # If it already passed the check, then let's save the columns here
            # TO BE DISPLAYED LATER ON: column_length, column_uniques_length, length_diff_ori_minus_unique
            
            if len(key_list) == 1:  # single key (just one string)

                # Defining the column, its values and its unique values
                col = key_list[0]
                series = df_name[col]
                uniques_list = series.dropna().astype(str).unique().tolist()

                # Filling our the elements of our dictionary
                key_mapping_column[key]["column"] = series
                key_mapping_column[key]["column_length"] = int(series.shape[0])
                key_mapping_column[key]["column_uniques"] = uniques_list
                key_mapping_column[key]["column_uniques_length"] = int(len(uniques_list))
                key_mapping_column[key]["length_diff_ori_minus_unique"] = int(series.shape[0]) - int(len(uniques_list))

                # Save then into our previously defined dictionary
                results[key] = {
                    f"key_column_{key_dictionary}_length":int(series.shape[0]),
                    f"key_column_{key_dictionary}_uniquevalues_length": int(len(uniques_list)),
                    f"key_column_{key_dictionary}_length_difference": int(series.shape[0]) - int(len(uniques_list))
                }
            
            else:
                # Multiple keys (mentions has two keys)
                results[key] ={}

                for e, col in enumerate(key_list, start=1):  # For the elements in key_dictionary
                    # Defining the column, its values and its unique values
                    series = df_name[col]
                    uniques_list = series.dropna().astype(str).unique().tolist()

                    # If it already passed the check, then let's save the columns here
                    # TO BE DISPLAYED LATER ON: column_length, column_uniques_length, length_diff_ori_minus_unique in an organized way
                    key_mapping_column[key][f"column{e}"] = series
                    key_mapping_column[key][f"column_length{e}"] = int(series.shape[0])
                    key_mapping_column[key][f"column_uniques{e}"] = uniques_list
                    key_mapping_column[key][f"column_uniques_length{e}"] = int(len(uniques_list))
                    key_mapping_column[key][f"length_diff_ori_minus_unique{e}"] = int(series.shape[0]) - int(len(uniques_list))

                    # Save then into our previously defined dictionary
                    results[key][f"key_column_{e}_{col}_length"] = int(series.shape[0]) # key_dictionary
                    results[key][f"key_column_{e}_{col}_uniquevalues_length"] = int(len(uniques_list))
                    results[key][f"key_column_{e}_{col}_length_difference"] = int(series.shape[0]) - int(len(uniques_list))
    
        # Return the results to be printed
        return results

    """ This one is
    Depending on the joint we will see how well the map did and in an excel
    and we will also check a one:HOWMany relationship was in the mapping """

    def key_cols_mapping_checkup(
        self,
        gkg_df: Optional[pd.DataFrame] = None,
        mentions_df: Optional[pd.DataFrame] = None,
        export_df: Optional[pd.DataFrame] = None
    ) -> Dict[str, pd.DataFrame]:

        """
        Map key columns between dataframes and check for repetitions.
        
        Inputs:
            gkg_df: GKG dataframe (optional depending on joincase)
            mentions_df: Mentions dataframe (optional depending on joincase)
            export_df: Export dataframe (optional depending on joincase)
        
        Returns:
            Dictionary of dataframes showing mapping counts between files
        """
                    
        # If no dictionary is provided, return empty results
        if not self.key_column_dictionary_document:
            self.logger.info("No key column dictionary provided, skipping mapping checkup")
            return {}

        # We need again another dictionary to save the the data frames
        df_dict: Dict[str, pd.DataFrame] = {}

        # CASE 1: gkg vs mentions (if both are provided, gkg is always provided) ----------------------->
        if gkg_df is not None and mentions_df is not None and "gkg" in self.key_column_dictionary_document and "mentions" in self.key_column_dictionary_document:
            
            # To get the gkg column and the mentions column
            gkg_column_name = self.key_column_dictionary_document["gkg"]
            mentions_column_name = self.key_column_dictionary_document["mentions"]
            
            # Handle mentions column (could be a list with two columns or a single string)
            if isinstance(mentions_column_name, list):
                mentions_column1_name = mentions_column_name[0]
            else:
                mentions_column1_name = mentions_column_name
            
            # Getting our specific columns
            gkg_column = gkg_df[gkg_column_name]
            mentions_column1 = mentions_df[mentions_column1_name]

            # Create our data frame columns
            out_col_names = [gkg_column_name, "mapped mentions values", "mapped count"]
            
            # Group mentions by gkg key into lists (only valid mentions)
            valid_mentions_mask = mentions_column1.notna() & mentions_column1.astype(str).str.strip().ne("")
            mentions_by_key = (
                mentions_df.loc[valid_mentions_mask]
                           .groupby(mentions_column1_name, sort=False)[mentions_column1_name]
                           .apply(list)  # keep duplicates
            )

            # Map the grouped lists back to each row's gkg key; fill missing with empty list
            mapped_lists = (
                gkg_column
                .map(mentions_by_key)
                .apply(lambda v: v if isinstance(v, list) else [])
            )
            
            # Count per row
            mapped_counts = mapped_lists.str.len()

            # Build output (same length as df)
            out = pd.DataFrame({
                out_col_names[0]: gkg_column.values,
                out_col_names[1]: mapped_lists,
                out_col_names[2]: mapped_counts
            })

            # Save it into the dictionary
            df_dict["gkg_vs_mentions"] = out
        
        # CASE 2: gkg vs export (if both are provided, if on top also export is provided) ----------------------->
        if gkg_df is not None and export_df is not None and "gkg" in self.key_column_dictionary_document and "export" in self.key_column_dictionary_document:
            
            # To get the gkg column and the export column
            gkg_column_name = self.key_column_dictionary_document["gkg"]
            export_column_name = self.key_column_dictionary_document["export"]
            
            # Handle export column (could be a list with two columns or a single string)
            if isinstance(export_column_name, list):
                export_column1_name = export_column_name[0]
            else:
                export_column1_name = export_column_name
            
            # Getting our specific columns
            gkg_column = gkg_df[gkg_column_name]
            export_column1 = export_df[export_column1_name]

            # Create our data frame columns
            out_col_names = [gkg_column_name, "mapped export values", "mapped count"]
            
            # Group mentions by gkg key into lists (only valid mentions)
            valid_export_mask = export_column1.notna() & export_column1.astype(str).str.strip().ne("")
            export_by_key = (
                export_df.loc[valid_export_mask]
                           .groupby(export_column1_name, sort=False)[export_column1_name]
                           .apply(list)  # keep duplicates
            )

            # Map the grouped lists back to each row's gkg key; fill missing with empty list
            mapped_lists = (
                gkg_column
                .map(export_by_key)
                .apply(lambda v: v if isinstance(v, list) else [])
            )
            
            # Count per row
            mapped_counts = mapped_lists.str.len()

            # Build output (same length as df)
            out = pd.DataFrame({
                out_col_names[0]: gkg_column.values,
                out_col_names[1]: mapped_lists,
                out_col_names[2]: mapped_counts
            })

            # Save it into the dictionary
            df_dict["gkg_vs_export"] = out

        # CASE 3: mentions vs export (if both are provided, if on top also export is provided) ----------------------->
        if mentions_df is not None and export_df is not None and "mentions" in self.key_column_dictionary_document and "export" in self.key_column_dictionary_document:
            
            # To get the mentions column and export column
            mentions_column_name = self.key_column_dictionary_document["mentions"]
            export_column_name = self.key_column_dictionary_document["export"]
            
            # Handle mentions column (if it's a list, take the second element for GlobalEventID)
            if isinstance(mentions_column_name, list):
                mentions_column2_name = mentions_column_name[1] if len(mentions_column_name) > 1 else mentions_column_name[0]
            else:
                mentions_column2_name = mentions_column_name
            
            # Getting our specific columns
            mentions_column2 = mentions_df[mentions_column2_name]
            export_column = export_df[export_column_name]

            # Create our data frame columns
            out_col_names = [mentions_column2_name, "mapped export values", "mapped count"]
            
            # Group mentions by gkg key into lists (only valid mentions)
            valid_export_mask = export_column.notna() & export_column.astype(str).str.strip().ne("")
            exports_by_key = (
                export_df.loc[valid_export_mask]
                         .groupby(export_column_name, sort=False)[export_column_name]
                         .apply(list)  # keep duplicates
            )

            # Map the grouped lists back to each row's gkg key; fill missing with empty list
            mapped_lists = (
                mentions_column2
                .map(exports_by_key)
                .apply(lambda v: v if isinstance(v, list) else [])
            )
            
            # Count per row
            mapped_counts = mapped_lists.str.len()

            # Build output (same length as df)
            out = pd.DataFrame({
                out_col_names[0]: mentions_column2.values,
                out_col_names[1]: mapped_lists,
                out_col_names[2]: mapped_counts
            })

            # Save it into the dictionary
            df_dict["mentions_vs_exports"] = out

        return df_dict

"""
FOURTH CLASS: DataJoiner
NOTICE: THIS CLASS CONTAINS A SERIES OF FUNCTIONS THAT ARE USED TO MERGE A DEFINED
LIST OF COLUMNS FROM MENTIONS AND EXPORT TO GKG
HOWEVER; DEPENDING ON THE JOINT, THE MERGING IS PERFORMED

HERE THE SET KEY COLUMNS FOR THE MAPPING OF EACH FILE
GKG: gkg_V2DOCUMENTIDENTIFIER
MENTIONS: MentionIdentifier
EXPORT: GlobalEventID
"""

class DataJoiner:
    """Joins GKG, Mentions, and Export data"""
    
    def __init__(self, mentions_columns: list[str], export_columns: list[str], temp_db_path: str = ":memory:"):
        self.mentions_columns = mentions_columns # List of columns from mentions that we would like to map
        self.export_columns = export_columns  # List of columns from export that we would like to map
        self.temp_db_path = temp_db_path
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def join(
        self,
        gkg_df: pd.DataFrame,
        mentions_df: Optional[pd.DataFrame] = None, # Can be given or not
        export_df: Optional[pd.DataFrame] = None, # Can be given or not
    ) -> pd.DataFrame:
        
        """
        Join GKG data with optional Mentions and Export dataframes.
        
        Inputs:
            gkg_df: GKG dataframe (required)
            mentions_df: Mentions dataframe (optional)
            export_df: Export dataframe (optional)
        
        Returns:
            Joined dataframe with selected columns from mentions and/or export
        """
        # Check if dataframes are provided
        has_mentions = mentions_df is not None
        has_export = export_df is not None
        
        # CASE 1: Just gkg, then return just gkg
        if not has_mentions and not has_export:
            self.logger.info(f"Returning gkg only: {len(gkg_df)} rows")
            return gkg_df
        
        # For all other cases (gkg + mentions, gkg + export, gkg + mentions + export) SQL joins are used
        # We call the function to create a virtual sqlite query
        df = self._perform_sql_join(
            gkg_df=gkg_df,
            mentions_df=mentions_df,
            export_df=export_df,
            mentions_columns=self.mentions_columns,
            export_columns=self.export_columns
        )

        return df
    
    def _perform_sql_join(
        self,
        gkg_df: pd.DataFrame,
        mentions_df: Optional[pd.DataFrame],
        export_df: Optional[pd.DataFrame],
        mentions_columns: Optional[List[str]],
        export_columns: Optional[List[str]]
    ) -> pd.DataFrame:

        """Perform SQL-based join operations opening a virtual sqlite database"""
        
        # Connect to the sqlite3 server
        with sqlite3.connect(self.temp_db_path) as conn:
            
            # Clean and load gkg
            gkg_df.columns = [c.strip() for c in gkg_df.columns]
            gkg_df.to_sql("gkg", conn, if_exists="replace", index=False)
            
            # Build the query dynamically
            query_parts = ["SELECT g.*"] # All the columns, because before we already deleted unwanted gkg columns in GKGProcessor class
            
            # In case there a mentions df is given
            if mentions_df is not None:

                # Clean and load mentions
                mentions_df.columns = [c.strip() for c in mentions_df.columns]
                mentions_df.to_sql("mentions", conn, if_exists="replace", index=False)
                
                # Get columns to select
                m_cols = self._get_mentions_columns(mentions_columns)
                query_parts.append(self._build_column_selection(m_cols, "m", "Mentions"))
            
            # In case an export df is given
            if export_df is not None:

                # Clean and load export
                export_df.columns = [c.strip() for c in export_df.columns]
                export_df.to_sql("export", conn, if_exists="replace", index=False)
                
                # Get columns to select
                e_cols = self._get_export_columns(export_columns)
                query_parts.append(self._build_column_selection(e_cols, "e", "Export"))
            
            # Build FROM and JOIN clauses
            query = ",\n    ".join(query_parts) + "\nFROM gkg AS g"
            
            # In case mentions df is given then join gkg + mentions
            if mentions_df is not None:
                # Join mentions to gkg
                query += """
LEFT JOIN mentions AS m
    ON TRIM(CAST(g.gkg_V2DOCUMENTIDENTIFIER AS TEXT)) = TRIM(CAST(m.MentionIdentifier AS TEXT))"""
            
             # In case export df is given then
            if export_df is not None:
                # An also mentions df is given
                if mentions_df is not None:
                    # Join export through mentions
                    query += """
LEFT JOIN export AS e
    ON TRIM(CAST(m.GlobalEventID AS TEXT)) = TRIM(CAST(e.GlobalEventID AS TEXT))"""

            # If only export df is given then
                else:
                    # Join export to gkg
                    query += """
LEFT JOIN export AS e
    ON TRIM(CAST(g.gkg_V2DOCUMENTIDENTIFIER AS TEXT)) = TRIM(CAST(e.SOURCEURL AS TEXT))"""
            
            # Execute query and save it
            result = pd.read_sql_query(query, conn)
        
        # Print the result
        self.logger.info(f"Joined data: {len(result)} rows, {len(result.columns)} columns")

        return result

    # SUPPORT FUNCTION ---> with predefined columns to map from mentions
    def _get_mentions_columns(self, custom_columns: Optional[List[str]] = None) -> List[str]:
        
        """Get the list of columns to map from mentions table"""

        if custom_columns:
            return custom_columns
        
        # Default columns
        return [
            "MentionDocTone"
        ]
    
    # SUPPORT FUNCTION ---> with predefined columns to map from export
    def _get_export_columns(self, custom_columns: Optional[List[str]] = None) -> List[str]:
        
        """Get the list of columns to map from export table"""

        if custom_columns:
            return custom_columns
        
        # Default columns
        return [
            "Actor1Code",
            "Actor1Name",
            "Actor1Geo_Type",
            "Actor1Geo_Fullname",
            "Actor1Geo_CountryCode",
            "NumMentions",
            "GoldsteinScale",
            "AvgTone"
        ]
    
    # SUPPORT FUNCTION ---> to build a string with the columns such as 
    # using the prefix plus the columns plus the table alias
    # so it looks like table_alias.columns[i] for all columns in columns
    def _build_column_selection(
        self,
        columns: List[str],
        table_alias: str,
        prefix: str
    ) -> str:

        """
        Build SQL column selection string with aliasing
        
        Args:
            columns: List of column names to select
            table_alias: SQL table alias (examples: 'm' (mentions), 'e' (export))
            prefix: Prefix for column aliases (examples:, 'Mentions', 'Export')
        
        Returns:
            Comma-separated SQL column selection string
        """

        selections = []
        for col in columns:
            # Special handling for GlobalEventID to avoid confusion
            if col == "GlobalEventID" and prefix == "Export":
                alias = "ExportANDMentions_GlobalEventID"
            else:
                alias = f"{prefix}_{col}"
            
            selections.append(f"{table_alias}.{col} AS {alias}")
        
        return ",\n    ".join(selections)


"""
FIFTH CLASS: MappingAnalyzer
NOTICE: THIS CLASS CONTAINS A SERIES OF FUNCTIONS TO GET THE COUNT OF ELEMENTS FROM THE TONES IN
MENTIONS AND EXPORT FILES FOR WHICH NO MATCH IN THE GKG WAS FOUND.
THIS IS IMPORTANT TO GET AN IDEA ON THE ACTUAL SIZE OF OUR DATA SET
"""
class MappingAnalyzer:

    """Analyzes mapping quality and completeness."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def analyze_unmapped_tones(
        self,
        df: pd.DataFrame,
        tone_columns: List[str],
        identifier_column: str
    ) -> Dict[str, Dict[str, Any]]:

        """
        Analyze unmapped tone values.
        
        Inputs:
            df: Joined dataframe to analyze
            tone_columns: List of column names to check for empty/unmapped values
            identifier_column: Column name used as identifier (e.g., gkg_V2DOCUMENTIDENTIFIER)
        
        Returns:
            Dictionary with statistics about empty/unmapped values per column
        """

        # Step 1: Work with the identifier column -------------------------------------------------------------->

        # Validate inputs the column given exists in the df
        if identifier_column not in df.columns:
            raise ValueError(f"Identifier column '{identifier_column}' not found")
        
        # Build mask of rows where the identifier column is filled for non blanks and non NAs
        id_series = df[identifier_column]
        id_filled_mask = id_series.notna() & (id_series.astype(str).str.strip() != "")

        # This will get us the row indexes for which a value is to find in the identifier column
        filled_indexes = df.index[id_filled_mask]
        
        # Step 2: Create a dictionary in which the number of not mapped elements will be saved and displayed ---->
        results = {}

        # Step 3: Loop through target columns ------------------------------------------------------------------->
        for col in tone_columns:
            # Column existence, if not the number of empty rows is set as None and we get a message that the column was not found
            if col not in df.columns:
                results[col] = {
                    "Empty": None,
                    "as_%_of_total_rows": None,
                    "note": "Column not found"
                }
                continue

             # Restrict to rows where identifier is filled

            # This step ensures that we only get the elements of the column for which
            # The column identifier in gkg was not empty
            # Why? Because if the column identifier "the key mapping column in gkg" was empty
            # It is clear that nothing from mentions will be mapped in gkg for this rows
            # And so in turn will affect the maps for Export
            # So this are the actual not mapped columns because the identifier did not match
            # And not because the identifier was empty
            
            series = df.loc[filled_indexes, col]

            # Define another mask but this time for the column in question: NaN OR blank string after stripping
            empty_mask = series.isna() | (series.astype(str).str.strip() == "")
            
            # Count the number of empty rows found
            empty_count = int(empty_mask.sum())
            # Count the number of rows found
            total_rows = int(series.shape[0])
            # Get the ratio between both of them
            ratio = (empty_count / total_rows) if total_rows > 0 else 0.0
            
            # Save then into our previously defined dictionary
            results[col] = {
                "Empty": empty_count,
                "as_%_of_total_rows": f"{ratio * 100:.2f}%",
                "rows_checked": total_rows
            }
        
        # Return the results
        return results
        # note: If required we could also check for which gkg_identifiers where the other columns not mapped
        #return not_mapped_elements

"""
SIXTH CLASS: GDELTDataLoader
NOTICE: THIS CLASS CONTAINS MAPS THE DICTIONARIES PER EACH FILE gkg, mentions, export
TO BE THE NEW HEADERS PER DOCUMENT. IT ALSO PULLS THE FILES FROM THE WEBSITE (THE ONES REQUIRED. EXAMPLE: only gkg)

PLEASE NOTICE THAT THIS FUNCTION ASSUMES.
1. A WELL STRUCTURED DICTIONARY FILEWITH SHEETS NAMES = gkg, mentions, export
2. ACCESS TO THE CORRECT GDELT SITE IN THE FUNCTION TO DOWNLOAD DATA
"""

class GDELTDataLoader:

    """Loads GDELT data files with proper headers from dictionary"""
    
    def __init__(self, dictionary_path: str):
        self.dictionary_path = Path(dictionary_path)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._load_dictionaries()
    
    def _load_dictionaries(self):

        """Load column dictionaries from Excel file"""

        # Import the dictionary database that we already saved
        # The dictionary is in a format of Sheet Name which cohtains the
        # Dictionary for that specific data
        self.dict_excel = pd.ExcelFile(self.dictionary_path)
        self.dictionaries = {}
        
        # For this list of sheets in my dictionary, they are already fixed
        for sheet_name in ["mentions", "export", "gkg"]:
            # Read the corresponding sheet from the dictionary Excel
            dict_df = pd.read_excel(self.dict_excel, sheet_name=sheet_name)
            # Take column A starting from row 2 (index 1) as headers, remember that the dictionaries also have headers
            headers = dict_df.iloc[0:, 0].dropna().tolist()
            self.dictionaries[sheet_name] = headers
    
    def load_file(self, file_path: str, file_type: str) -> pd.DataFrame:

        """
        Load a single GDELT file with appropriate headers

        Args:
            path: location in the computer of the file to be loaded
        
        Returns:
            A data frame
        """

        # This is to match gkg, mentions, export with their dictionaries
        # Is the list/name was not correctly defined, name give != a sheet in the dictionaries
        # This will raise an error
        if file_type not in self.dictionaries:
            raise ValueError(f"Unknown file type: {file_type}")
        
        # Save the data frames
        df = pd.read_csv(file_path, delimiter="\t", quoting=3, engine="python")
        headers = self.dictionaries[file_type]
        
        # Apply new headers to the DataFrame only if they have the same length
        if len(headers) != df.shape[1]:
            # Show the mismatch
            self.logger.warning(
                f"Header mismatch for {file_type}: CSV has {df.shape[1]} columns, "
                f"dictionary has {len(headers)} headers"
            )
        # In case no mismatch of length was found, map the dictionaries as new headers
        else:
            df.columns = headers
        
        # Display info
        self.logger.info(f"Loaded {file_type}: {len(df)} rows")

        # Return the df
        return df
    
    def download_gdelt_files(self, timestamp_key: str, files_to_download: List[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Download GDELT files for a specific timestamp from the GDELT website.
        
        Inputs:
            timestamp_key: Timestamp in format YYYYMMDDHHMMSS (e.g., '20160218230000')
            files_to_download: List of file types to download (e.g., ['gkg', 'mentions', 'export'])
                             If None, downloads all three files
        
        Returns:
            Dictionary with keys 'export_df', 'mentions_df', 'gkg_df' containing the DataFrames
        
        Example:
            # Download only GKG
            data = gdelt.download_gdelt_files('20160218230000', files_to_download=['gkg'])
            gkg_df = data['gkg_df']
            
            # Download GKG and mentions
            data = gdelt.download_gdelt_files('20160218230000', files_to_download=['gkg', 'mentions'])
            
            # Download all files
            data = gdelt.download_gdelt_files('20160218230000')
        """

        # Increase CSV field size limit to handle large GDELT fields
        #import csv
        #import sys
        maxInt = sys.maxsize
        while True:
            try: 
                csv.field_size_limit(maxInt)
                break
            except OverflowError:
                maxInt = int(maxInt/10)

        # Base URL for GDELT v2 data where the data is stored
        base_url = "http://data.gdeltproject.org/gdeltv2/"
        
        # Define file types and their corresponding dictionary keys
        all_file_configs = {
            'export_df': { # this is how the data frame will be saved in the end
                'suffix': '.export.CSV.zip', # name and extension of the file as per on the GDELT site
                'dict_key': 'export', # key to save in the dictionary of data frames
                'csv_name_pattern': '.export.CSV' # csv patter after being unzipped
            },
            'mentions_df': {
                'suffix': '.mentions.CSV.zip',
                'dict_key': 'mentions',
                'csv_name_pattern': '.mentions.CSV'
            },
            'gkg_df': {
                'suffix': '.gkg.csv.zip',
                'dict_key': 'gkg',
                'csv_name_pattern': '.gkg.csv'
            }
        }
        
        # If files_to_download is specified, filter the configurations
        if files_to_download is not None: # If not empty
            file_configs = {} # Create a dictionary
            for file_type in files_to_download: # Save a name for the dictionary to be used as key for the df
                df_name = f"{file_type}_df"
                if df_name in all_file_configs:
                    file_configs[df_name] = all_file_configs[df_name] # Save each in the dictionary
                else: # File types can only be gkg, mentions, export
                    raise ValueError(f"Unknown file type: {file_type}. Must be one of: gkg, mentions, export")
        else:
            # Download all files if not specified
            file_configs = all_file_configs
        
        # To save the data frames in a dictionary
        result = {}
        
        # Download and process each file type
        for df_name, config in file_configs.items():
            try:
                # Construct the full URL
                file_url = f"{base_url}{timestamp_key}{config['suffix']}"
                
                # Display what is being downloaded
                self.logger.info(f"Downloading {df_name} from {file_url}")
                
                # Download the zip file
                response = requests.get(file_url, timeout=30)
                response.raise_for_status()  # Raise exception for bad status codes
                
                # Extract and read the CSV from the zip file
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    # Get the CSV filename (only one file in the zip)
                    csv_filename = [name for name in zip_file.namelist()
                                if config['csv_name_pattern'] in name][0]
                    
                    # Read the CSV file
                    with zip_file.open(csv_filename) as csv_file:
                        df = pd.read_csv(csv_file, delimiter="\t", quoting=3, engine="python")
                        
                        # Get the appropriate headers from dictionaries
                        file_type = config['dict_key']
                        if file_type not in self.dictionaries:
                            raise ValueError(f"Unknown file type: {file_type}")
                        
                        # These are the headers in the dictionaries to be applied
                        headers = self.dictionaries[file_type]
                        
                        # Apply headers if they match
                        if len(headers) != df.shape[1]:
                            self.logger.warning(
                                f"Header mismatch for {file_type}: CSV has {df.shape[1]} columns, "
                                f"dictionary has {len(headers)} headers"
                            )
                        else:
                            df.columns = headers
                        
                        # Displayed the name of the df that was loaded and how many rows does it have
                        self.logger.info(f"Loaded {df_name}: {len(df)} rows")
                        
                        # Store in result dictionary
                        result[df_name] = df

            # If the timestamp_key is not found            
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error downloading {df_name} for timestamp {timestamp_key}: {e}")
                raise
            # If the file within the zip could not be extracted
            except zipfile.BadZipFile as e:
                self.logger.error(f"Error extracting zip file for {df_name}: {e}")
                raise
            # Other processing error
            except Exception as e:
                self.logger.error(f"Unexpected error processing {df_name}: {e}")
                raise
        
        # Show that all files where downloaded
        self.logger.info(f"Successfully downloaded {len(result)} file(s) for timestamp {timestamp_key}")
        return result

    


"""
SEVENTH CLASS: GDELTProcessor
This is the main class using the variables from all other classes to process the GDELT Data
"""
class GDELTProcessor:
    
    """Main function for GDELT data processing"""
    
    # This defines the inputs required for this function
    def __init__(
        self,
        dictionary_path: str,
        output_dir: str,
        country_codes: Optional[List[str]] = None,
        themes_tags: Optional[List[str]] = None,
        gkg_columns_to_drop: Optional[List[str]] = None,
        mentions_columns_to_map: Optional[List[str]] = None,
        export_columns_to_map: Optional[List[str]] = None
    ):  

        # This part is to define functions and variables from classes that will be used here
        # Some of the function with inputs will be defined later
        self.loader = GDELTDataLoader(dictionary_path)
        self.gkg_processor = GKGProcessor(gkg_columns_to_drop)
        # Note: keycolumn_checkup and joiner classes will be initialized per fileset with appropriate key_column_dictionary
        self.analyzer = MappingAnalyzer()
        self.country_codes = country_codes
        self.themes_tags = themes_tags
        self.mentions_columns_to_map = mentions_columns_to_map
        self.export_columns_to_map = export_columns_to_map
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    # This functions has two arguments:
    # 1. self: It represents the instance of the class
    # 2. fileset: GDELTFileSet --> This is a parameter defined with timestamp, joincase, statistics, and key_column_dictionary_document
    # 3. Optional[GDELTMappingQuality] in case we want to check the statistics of the mapping of the documents
    def process_fileset(
        self,
        fileset: GDELTFileSet,
        mapping_columns: Optional[GDELTMappingQuality] = None
    ) -> Union[pd.DataFrame, Tuple]:
        """
        Process a complete set of GDELT files (gkg, mentions, export) based on joincase.
        
        Inputs:
            fileset: GDELTFileSet object containing:
                - timestamp: str (e.g., '20251201143000')
                - joincase: str ('gkg_only', 'gkg_mentions', 'gkg_export', 'all')
                - statistics: str ('all', 'key_columns_stats', 'none')
                - key_column_dictionary_document: Dict specifying key columns for each file
            mapping_columns: GDELTMappingQuality object (required if statistics != 'none')
        
        Returns:
            Depending on statistics parameter:
            - 'none': joined_df
            - 'key_columns_stats': (df_key_columns_stats, joined_df)
            - 'all': (key_columns_stats, df_key_columns_stats, joined_df, mapping_stats)
        """

        self.logger.info(f"Processing fileset for timestamp: {fileset.timestamp}")

        # Initialize key column checkup and joiner with fileset's key column dictionary
        # These are initialized here because the key columns depend on the joincase
        keycolumn_checkup = KeyColumnsCheckUp(fileset.key_column_dictionary_document)
        joiner = DataJoiner(self.mentions_columns_to_map, self.export_columns_to_map)

        # STEP 0: Determine which files to download based on joincase ------------------
        files_to_download = self._get_files_for_joincase(fileset.joincase)
        
        # Download only the required files from GDELT site
        data = self.loader.download_gdelt_files(fileset.timestamp, files_to_download=files_to_download)

        # STEP 1: Process gkg (this file is always to be included) ------------------
        gkg_raw = data['gkg_df']

        # Now if country code is different to none check filter out column V2ENHANCEDLOCATIONS
        if self.country_codes: # If they were given
            if 'V2ENHANCEDLOCATIONS' in gkg_raw.columns: # If the column exists
            
                # Build a safe regex pattern of the form: (?<=#)(US|MX|CA)(?=#)
                codes = [str(c) for c in self.country_codes]  # ensure strings
                pattern = r'(?<=#)(' + '|'.join(map(re.escape, codes)) + r')(?=#)' # Anything between #...#

                # Filter rows where any of the desired codes appears between #...#
                mask = gkg_raw['V2ENHANCEDLOCATIONS'].str.contains(pattern, regex=True, na=False) # This is the filter
                gkg_raw = gkg_raw[mask]
        
        # Process GKG
        gkg_processed = self.gkg_processor.process(gkg_raw)

        # STEP ADDED TO REDUCE THE COLUMNS OF gkg resulting from gkg_processor ----
        # List of extra gkg columns to be deleted
        gkg_to_be_deleted = ["gkg_V1THEMES", "gkg_V2ENHANCEDTHEMES", "gkg_V1.5TONE", "gkg_V1NUMBERS_list_str", "gkg_V2NUMBERS_list_str"]
        gkg_processed = gkg_processed.drop(columns=gkg_to_be_deleted, errors='ignore')

        # Filter out themes that we actually want to have in gkg, but that begin with a tag
        if self.themes_tags: # If they were given
            if 'gkg_V2ENHANCEDTHEMES_list_str' in gkg_processed.columns: # If the column exists
                
                # The next steps are to just choose lines in colum 'gkg_V2ENHANCEDTHEMES_list_str
                # Whose Themes begins with one of the tags in list "themes_tags"
                # To note: 
                # As each line in gkg contains a lot of themes, this filter may not be that strong
                # Because the variety of tags per line is huge

                # Normalize tags to strings and upper (if your tokens are upper-case)
                tags = tuple(str(tag).strip() for tag in self.themes_tags)

                # Build a boolean mask: True if any token starts with any given tag
                mask = (
                    gkg_processed['gkg_V2ENHANCEDTHEMES_list_str']
                    .fillna('')
                    .str.split(',')
                    .apply(lambda toks: any(t.strip().startswith(tags) for t in toks))
                )

                gkg_processed = gkg_processed[mask]
        
        # STEP 2: Get the required dataframes based on joincase -----------------------
        mentions_raw = data.get('mentions_df', None)
        export_raw = data.get('export_df', None)

        # Filter out if country_codes were given as an input and if column 'Actor1Geo_CountryCode' is to be found
        if self.country_codes: # If they were given
            if 'Actor1Geo_CountryCode' in export_raw.columns: # If the column exists
                export_raw = export_raw[export_raw['Actor1Geo_CountryCode'].isin(self.country_codes)]
        
        # STEP 3: Join data ------------------------------------------------------------
        joined_df = joiner.join(
            gkg_df=gkg_processed,
            mentions_df=mentions_raw,
            export_df=export_raw
        )
        
        # Add time stamp as the first column
        joined_df.insert(0, 'Time Stamp', fileset.timestamp)
        
        # STEP 4: Handle statistics based on statistics parameter ---------------------
        if fileset.statistics == "none":
            # Return only the joined dataframe
            self.logger.info(f"Completed processing for {fileset.timestamp} (no statistics)")
            return joined_df
            
        elif fileset.statistics == "key_columns_stats":
            # Generate key column statistics only
            df_key_columns_stats = keycolumn_checkup.key_cols_mapping_checkup(
                gkg_df=gkg_processed,
                mentions_df=mentions_raw,
                export_df=export_raw
            )

            # Add time stamp as the first column
            for name, df in df_key_columns_stats.items():
                df.insert(0, 'Time Stamp', fileset.timestamp)
            
            self.logger.info(f"Completed processing for {fileset.timestamp} (key columns stats only)")
            return df_key_columns_stats, joined_df
            
        elif fileset.statistics == "all":
            # Validate that mapping_columns is provided
            if mapping_columns is None:
                raise ValueError("mapping_columns parameter is required when statistics='all'")
            
            # Generate all statistics
            # -> Statistics shown in the console ---------
            key_columns_stats = keycolumn_checkup.check_key_columns(
                gkg_df=gkg_processed,
                mentions_df=mentions_raw,
                export_df=export_raw
            )
            
            # Analyze mapping quality: Define the tone columns to be used to count their empty mapped values
            mapping_stats = self.analyzer.analyze_unmapped_tones(
                joined_df,
                mapping_columns.checkmapping_cols,
                mapping_columns.identifier_col
            )
            
            # -> Statistics saved in a df ----------------
            df_key_columns_stats = keycolumn_checkup.key_cols_mapping_checkup(
                gkg_df=gkg_processed,
                mentions_df=mentions_raw,
                export_df=export_raw
            )

            # Add time stamp as the first column 
            for name, df in df_key_columns_stats.items():
                df.insert(0, 'Time Stamp', fileset.timestamp)
            
            self.logger.info(f"Completed processing for {fileset.timestamp} (all statistics)")
            return key_columns_stats, df_key_columns_stats, joined_df, mapping_stats
        
        else:
            raise ValueError(f"Invalid statistics parameter: {fileset.statistics}. Must be 'all', 'key_columns_stats', or 'none'")
    
    # This is a suport function for the STEP 0 in the above function
    # Depending on the joincase input, do we retrieve the corresponding df(s)
    def _get_files_for_joincase(self, joincase: str) -> List[str]:
        """
        Determine which files need to be downloaded based on joincase.
        
        Inputs:
            joincase: Type of join ('gkg_only', 'gkg_mentions', 'gkg_export', 'all')
        
        Returns:
            List of file types to download (e.g., ['gkg'], ['gkg', 'mentions'], etc.)
        """
        if joincase == "gkg_only":
            return ['gkg']
        elif joincase == "gkg_mentions":
            return ['gkg', 'mentions']
        elif joincase == "gkg_export":
            return ['gkg', 'export']
        elif joincase == "all":
            return ['gkg', 'mentions', 'export']
        else:
            raise ValueError(f"Unknown joincase: {joincase}. Must be one of: gkg_only, gkg_mentions, gkg_export, all")
    
    # To save the df with the mapping logic, what was mapped and what is the mapping logic sequence 1:TOHOWMANY
    def save_key_columns_analysis(self, df_dic: Dict[str, pd.DataFrame], timestamp: str, format: str = "xlsx"):
        
        """
        Save key columns analysis results to the computer.

        Inputs:
            df_dic: Dictionary of dataframes to save (each becomes a sheet for Excel; 
                    for pickle the whole dict is saved as a single object)
            timestamp: Timestamp string to include in filename
            format: File format ('xlsx', 'xlsm', 'pkl', or 'pickle')

        Returns:
            Path to the saved file (file or folder, depending on format)
        """

        if not df_dic:
            self.logger.warning("No key column stats to save - skipping export")
            return None # Explicitely return none

        fmt = format.lower()
        if fmt not in {"xlsx", "xlsm", "pkl", "pickle"}:
            raise ValueError("Supported formats: 'xlsx', 'xlsm', 'pkl', 'pickle'.")

        # Build target path
        filename = f"Key_columns_checkup_{timestamp}.{('xlsx' if fmt=='xlsm' else fmt)}"
        filepath = self.output_dir / filename

        # Excel branch (multi-sheet)
        if fmt in {"xlsx", "xlsm"}:
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                for key, df in df_dic.items():
                    # Sanitize sheet name (<=31 chars and no invalid chars)
                    safe_sheet_name = (
                        str(key)
                        .replace(":", "_").replace("*", "_").replace("?", "_")
                        .replace("/", "_").replace("\\", "_")
                    )
                    safe_sheet_name = safe_sheet_name[:31]
                    df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
            self.logger.info(f"Saved results to {filepath}")
            return filepath

        # Pickle branch – save the ENTIRE dict as a single pickle file
        # (fastest; preserves dtypes/index exactly)
        with open(filepath, "wb") as f:
            pickle.dump(df_dic, f, protocol=pickle.HIGHEST_PROTOCOL)
        self.logger.info(f"Saved dictionary of DataFrames to {filepath} (pickle)")
        return filepath
    
    # To save the join df
    def save_results(self, df: pd.DataFrame, timestamp: str, format: str = "csv"):
        """
        Save processed results to the computer.

        Inputs:
            df: Dataframe to save
            timestamp: Timestamp string to include in filename
            format: File format ('csv', 'xlsx', 'parquet', 'pkl', 'pickle')

        Returns:
            Path to the saved file
        """
        fmt = format.lower()
        filename = f"GDELT_Joint_{timestamp}.{('xlsx' if fmt=='xlsm' else fmt)}"
        filepath = self.output_dir / filename

        if fmt == "csv":
            df.to_csv(filepath, index=False)
        elif fmt in {"xlsx", "xlsm"}:
            df.to_excel(filepath, index=False, engine="openpyxl")
        elif fmt == "parquet":
            df.to_parquet(filepath, index=False)  # preserves types; columnar; compressed
        elif fmt in {"pkl", "pickle"}:
            df.to_pickle(filepath)  # uses pickle protocol internally
        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.info(f"Saved results to {filepath}")
        return filepath


# New class to wrap up and use different time stamps =============================================

"""
EIGHT CLASS: GDELTTimestampBatchRunner
This will wrap my GDELTProcessor to process ranges of timestamps
And this will also save the resulting files in a unique file (one for statistics and one for the join file)
"""

class GDELTTimestampBatchRunner:

    """
    Wraps GDELTProcessor to handle:
    - a single timestamp, or
    - a timestamp range at 15-minute frequency,

    then aggregates outputs into:
    - one concatenated dataframe, and optionally
    - aggregated statistics outputs (depending on fileset.statistics value).
    """

    def __init__(self, processor: "GDELTProcessor"):

        """
        Args:
            processor: An already-configured GDELTProcessor instance
        """

        # This function casll my GDELTProcessor class
        self.processor = processor
        self.logger = logging.getLogger(self.__class__.__name__)

    # -------------------------------------------------------------------------------
    # Timestamp validation helpers
    # These functions help us to validate that the time has a correct format
    # YYYYMMDDHHMMSS : YYYY (year), MM (month), DD (Day)
    # MM (Minute, can only take 00, 15, 30, 45), SS (Seconds, can only take == value)
    # -------------------------------------------------------------------------------

    @staticmethod
    def _parse_ts(ts: str) -> datetime:
        """
        Parse YYYYMMDDHHMMSS into datetime.
        """
        return datetime.strptime(ts, "%Y%m%d%H%M%S")

    @staticmethod
    def _format_ts(dt: datetime) -> str:
        """
        Format datetime into YYYYMMDDHHMMSS.
        """
        return dt.strftime("%Y%m%d%H%M%S")

    @staticmethod
    def _validate_ts_rules(ts: str) -> None:

        """
        RULES:
        - must be valid timestamp format (delegate to your validate_timestamp)
        - seconds must be 00
        - minutes must be one of {00, 15, 30, 45}
        """

        # More check ups to validate minutes and seconds right formatting
        dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
        if dt.second != 0:
            raise ValueError(f"Invalid timestamp seconds (must be 00): {ts}")
        if dt.minute not in (0, 15, 30, 45):
            raise ValueError(f"Invalid timestamp minutes (must be 00/15/30/45): {ts}")

    def _expand_timestamps(
        self,
        timestamp_start: str,
        timestamp_end: Optional[str] = None
    ) -> List[str]:

        """
        Expand [start, end] into a list of timestamps every 15 minutes.

        Rules:
        - start is mandatory --> in case we just want to process one timestamp
        - end is optional --> if not given we just process one timestamp
        - if end is None -> single timestamp list [start]
        - end must be >= start
        - only 15-minute aligned timestamps allowed (enforced by _validate_ts_rules)
        """

        # First validate timestamp_start
        self._validate_ts_rules(timestamp_start)

        # Check if timestamp_end was given or not
        if timestamp_end is None or str(timestamp_end).strip() == "":
            return [timestamp_start]

        # If timestamp_end was given, validate it
        self._validate_ts_rules(timestamp_end)

        # After validation, define the timestamps formally
        start_dt = self._parse_ts(timestamp_start)
        end_dt = self._parse_ts(timestamp_end)

        # Check that timestamp_end is higher in value as date as timestamp_start
        if end_dt < start_dt:
            raise ValueError(
                f"timestamp_end must be >= timestamp_start. "
                f"Got start={timestamp_start}, end={timestamp_end}"
            )

        out = []
        current = start_dt
        step = timedelta(minutes=15)

        while current <= end_dt:
            out.append(self._format_ts(current))
            current += step

        return out

    # ---------------------------------------------------------------------------
    # Aggregation helpers: These ones will help us to join the data frames
    # ---------------------------------------------------------------------------

    @staticmethod
    def _concat_or_empty(frames: List[pd.DataFrame]) -> pd.DataFrame:

        """
        Concatenate frames safely; return empty dataframe if nothing was produced.
        """

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _merge_mapping_stats_dicts(
        acc: Dict[str, Any],
        ts: str,
        mapping_stats: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:

        """
        Store mapping_stats per timestamp in an accumulator dict.

        Output structure:
            acc[timestamp][column_name] = metrics_dict
        """

        if "mapping_stats_by_timestamp" not in acc:
            acc["mapping_stats_by_timestamp"] = {}
        acc["mapping_stats_by_timestamp"][ts] = mapping_stats
        return acc

    @staticmethod
    def _merge_key_columns_stats_dicts(
        acc: Dict[str, Any],
        ts: str,
        key_columns_stats: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:

        """
        Store key_columns_stats per timestamp.

        Output structure:
            acc[timestamp][file_key] = metrics_dict
        """
        
        if "key_columns_stats_by_timestamp" not in acc:
            acc["key_columns_stats_by_timestamp"] = {}
        acc["key_columns_stats_by_timestamp"][ts] = key_columns_stats
        return acc

    @staticmethod
    def _merge_df_key_columns_stats_dicts(
        acc: Dict[str, Any],
        ts: str,
        df_key_columns_stats: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:

        """
        Store the dataframe-based key-columns checkup outputs per timestamp.

        Output structure:
            acc[timestamp][sheet_name] = dataframe
        """

        if "df_key_columns_stats_by_timestamp" not in acc:
            acc["df_key_columns_stats_by_timestamp"] = {}
        acc["df_key_columns_stats_by_timestamp"][ts] = df_key_columns_stats
        return acc

    def _flatten_df_key_columns_stats_to_single_workbook_dict(
        self,
        df_key_stats_by_ts: Dict[str, Dict[str, pd.DataFrame]]
    ) -> Dict[str, pd.DataFrame]:

        """
        Concatenate per-timestamp dataframes vertically into single sheets.

        Example for gkg_export
            input:
                {
                "20251201143000": {"gkg_vs_export": df1},
                "20251201144500": {"gkg_vs_export": df2},
                }

            output:
                {
                "gkg_vs_export": pd.concat([df1, df2])  # All timestamps in one sheet
                }
        """

        # Group dataframes by sheet name
        sheets_dict: Dict[str, List[pd.DataFrame]] = {}
        
        # For each time stamps and sheets in df_key_stats_by_ts
        for ts, sheets in (df_key_stats_by_ts or {}).items():
            for sheet_name, df in (sheets or {}).items():
                if sheet_name not in sheets_dict:
                    sheets_dict[sheet_name] = []
                sheets_dict[sheet_name].append(df)
        
        # Concatenate all dataframes for each sheet vertically
        out: Dict[str, pd.DataFrame] = {}
        # Per each data frame
        for sheet_name, df_list in sheets_dict.items():
            if df_list:
                concatenated_df = pd.concat(df_list, ignore_index=True)
                out[sheet_name] = concatenated_df
        
        return out

    # -------------------------------------------
    # MAIN FUNCTION THAT WRAPS the GDELTProcessor
    # -------------------------------------------

    def run(
        self,
        base_fileset: "GDELTFileSet",
        mapping_columns: Optional["GDELTMappingQuality"] = None,
        timestamp_start: Optional[str] = None,
        timestamp_end: Optional[str] = None,
        on_error: Literal["raise", "skip"] = "raise",
        return_mode: Literal["match_processor", "always_dict"] = "always_dict",
        flatten_df_key_columns_stats: bool = True
    ) -> Any:

        """
        Run batch processing over one or many timestamps.

        Args:
            base_fileset:
                A GDELTFileSet instance providing joincase/statistics/key_column_dictionary_document.
                Its `timestamp` value is ignored if timestamp_start is provided.
            mapping_columns:
                Passed through to processor.process_fileset (required when statistics="all").
            timestamp_start:
                Mandatory if you want range-mode. If None, uses base_fileset.timestamp.
            timestamp_end:
                Optional. If None -> single timestamp.
            on_error:
                - "raise": stop immediately on first exception
                - "skip": log the error and continue (you'll get partial results)
            return_mode:
                - "match_processor": return exactly what processor returns but aggregated
                - "always_dict": always return dict with {"data", "stats", "timestamps_processed", ...}
            flatten_df_key_columns_stats:
                If True and you later want to save all stats in one Excel,
                flatten per-timestamp sheet dict into one sheet dict.

        Returns:
            If return_mode="always_dict", returns:
                {
                  "timestamps_requested": [...],
                  "timestamps_processed": [...],
                  "timestamps_failed": {ts: "error message", ...},
                  "joined_df": <pd.DataFrame>,
                  "stats": {
                      "key_columns_stats_by_timestamp": {...},        # only when statistics="all"
                      "mapping_stats_by_timestamp": {...},            # only when statistics="all"
                      "df_key_columns_stats_by_timestamp": {...},     # when statistics="all" or "key_columns_stats"
                      "df_key_columns_stats_flat": {...}              # optional flattened workbook dict
                  }
                }

            If return_mode="match_processor":
                - statistics="none" -> joined_df
                - statistics="key_columns_stats" -> (df_key_columns_stats_aggregated, joined_df)
                - statistics="all" -> (key_columns_stats_aggregated, df_key_columns_stats_aggregated, joined_df, mapping_stats_aggregated)
        """

        # Start the time counter
        start_time=time.time()

        # Decide the run timestamps
        # In GDELTProcessor we already defined a timestamp
        # However in case we give a timestamp_start as input, then the one in GDELTProcessor will be ignored
        if timestamp_start is None or str(timestamp_start).strip() == "":
            if not getattr(base_fileset, "timestamp", None):
                raise ValueError("Provide timestamp_start or set base_fileset.timestamp")
            timestamps = [base_fileset.timestamp]
        else:
            timestamps = self._expand_timestamps(timestamp_start, timestamp_end)

        # This will store our joined data frames
        joined_frames: List[pd.DataFrame] = []
        # A dictionary for my statistics
        stats_acc: Dict[str, Any] = {}
        # A list of the documents processed
        processed: List[str] = []
        # A dictionary of the failures to be processed
        failed: Dict[str, str] = {}

        # Now our loop for each time stamp comprised in our interval
        for ts in timestamps:
            try:
                # Create a new fileset per timestamp (do not mutate caller)
                fs = GDELTFileSet(
                    timestamp=ts,
                    joincase=base_fileset.joincase,
                    statistics=base_fileset.statistics,
                    key_column_dictionary_document=base_fileset.key_column_dictionary_document
                )

                # This saves the result of the mapping_columns
                result = self.processor.process_fileset(fs, mapping_columns)

                # Cases depending whether I want statistics or not

                # In case statistics == "none", just return the joined data frames
                if fs.statistics == "none":
                    joined_df = result
                    joined_frames.append(joined_df)

                # In case statistics == "key_columns_stats", just return the joined data frames and the df_key_cols_stats
                elif fs.statistics == "key_columns_stats":
                    df_key_cols_stats, joined_df = result
                    joined_frames.append(joined_df)
                    stats_acc = self._merge_df_key_columns_stats_dicts(stats_acc, ts, df_key_cols_stats)

                # In case statistics == "all", then return both dfs plus the statistics
                elif fs.statistics == "all":
                    key_columns_stats, df_key_cols_stats, joined_df, mapping_stats = result
                    joined_frames.append(joined_df)

                    stats_acc = self._merge_key_columns_stats_dicts(stats_acc, ts, key_columns_stats)
                    stats_acc = self._merge_df_key_columns_stats_dicts(stats_acc, ts, df_key_cols_stats)
                    stats_acc = self._merge_mapping_stats_dicts(stats_acc, ts, mapping_stats)

                # In case the value of statistiscs was not valid, raise an error
                else:
                    raise ValueError(f"Unknown statistics value: {fs.statistics}")

                processed.append(ts)

            # When a file for a timestamp could not be processed, then show which one failed
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                failed[ts] = msg
                self.logger.error(f"Failed timestamp {ts}: {msg}")

                if on_error == "raise":
                    raise
                # if "skip": continue

        # Final joined data frame
        final_joined = self._concat_or_empty(joined_frames)

        # Flattening for Excel saving (single workbook) for the df_key_columns_stats_by_timestamp
        if flatten_df_key_columns_stats and "df_key_columns_stats_by_timestamp" in stats_acc:
            stats_acc["df_key_columns_stats_flat"] = self._flatten_df_key_columns_stats_to_single_workbook_dict(
                stats_acc["df_key_columns_stats_by_timestamp"]
            )

        # Calculate total processing time -------------------------------------------
        elapsed_time = time.time() - start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)

        self.logger.info(f"Total processing time: {hours}h {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")
        # ---------------------------------------------------------------------------

        # If we want that wat is return is everything then use this
        if return_mode == "always_dict":
            return {
                "timestamps_requested": timestamps, # Timestamps within the given range
                "timestamps_processed": processed, # Timestamps that were actually processed
                "timestamps_failed": failed, # Time stamps that failed to be processed
                "joined_df": final_joined, # The joined df
                "stats": stats_acc, # The statistics of the key mapping for each df
                "processing_time_seconds": elapsed_time # To show how much time it took to process everything
            }

        # If we just need the return everything but not as a dictionary and do not show the timestamps_requested, timestamps_processed and timestamps_failed
        if return_mode == "match_processor":

            # CASES per statistics value (input)
            if base_fileset.statistics == "none":
                return final_joined

            if base_fileset.statistics == "key_columns_stats":
                # Return ONE workbook dict: either flat (preferred) or the nested per-ts dict
                if flatten_df_key_columns_stats and "df_key_columns_stats_flat" in stats_acc:
                    return stats_acc["df_key_columns_stats_flat"], final_joined
                return stats_acc.get("df_key_columns_stats_by_timestamp", {}), final_joined

            if base_fileset.statistics == "all":
                # For "match_processor" we return aggregated dicts by timestamp
                key_cols = stats_acc.get("key_columns_stats_by_timestamp", {})
                mapping = stats_acc.get("mapping_stats_by_timestamp", {})
                df_key_cols = stats_acc.get("df_key_columns_stats_flat", {}) if flatten_df_key_columns_stats else stats_acc.get("df_key_columns_stats_by_timestamp", {})
                return key_cols, df_key_cols, final_joined, mapping

            raise ValueError(f"Unknown statistics value: {base_fileset.statistics}")



