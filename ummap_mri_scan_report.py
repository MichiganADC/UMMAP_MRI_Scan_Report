#!/usr/bin/env python3

# ummap_mri_scan_report.py

# Import modules

import re
import numpy as np
import pandas as pd

import config as cfg
import helpers as hlps


# Helper function

def zero_to_nan(val):
    if val == 0 or val == 0.0 or val == "0" or val == "0.0":
        return np.nan
    return val


# Retrieve MRI scan data via REDCap API

fields_raw = ['subject_id', 'exam_date']
fields = ",".join(fields_raw)

forms_raw = ["mri_imaging_form"]
forms = ",".join(forms_raw)

df_ug = hlps.export_redcap_records(uri=cfg.REDCAP_API_URI,
                                   token=cfg.REDCAP_API_TOKEN_UMMAP_GENERAL,
                                   fields=fields,
                                   forms=forms)


# Select relevant columns

re_cols_scan = re.compile(r'^(scan|scanmrs)_.*[a-z].*_completed$')
cols_scan = df_ug.columns[df_ug.columns.str.match(re_cols_scan)]
cols_keep = fields_raw + list(cols_scan)

df_ug_slc = df_ug.loc[:, cols_keep]


# Filter out non-UMMAP IDs

re_ug_ids = re.compile(r'^UM\d{8}$')
bool_ug_ids = df_ug['subject_id'].str.match(re_ug_ids)

re_ug_dates = re.compile(r'^\d{4}-\d{2}-\d{2}$')
bool_ug_dates = df_ug['exam_date'].str.match(re_ug_dates)

df_ug_slc_flt = df_ug_slc.loc[bool_ug_ids & bool_ug_dates, :]. \
    dropna(axis='rows', how='all', subset=cols_scan). \
    reset_index(drop=True)


# Reconcile sequences that overlap between UMMAP and MRS

cols_scan_trim = \
    df_ug_slc_flt.columns.str.extract(r'^(scan|scanmrs)_(.*)_completed$')[1].dropna()

idx_cols_scan_trim_unique = cols_scan_trim.drop_duplicates(keep=False).index

cols_count = len(df_ug_slc_flt.columns)
cols_count_range = range(len(fields_raw), cols_count)

idx_cols_scan_trim_duplct = \
    pd.Index(filter(lambda x: x not in idx_cols_scan_trim_unique, cols_count_range))

cols_duplct = df_ug_slc_flt.columns[idx_cols_scan_trim_duplct]
cols_unique = df_ug_slc_flt.columns[idx_cols_scan_trim_unique]


# Coalesce sequence name columns that overlap between UMMAP and MRS

df_ug_coal = df_ug_slc_flt[['subject_id', 'exam_date']].copy()
df_ug_coal = pd.concat([df_ug_coal, df_ug_slc_flt.loc[:, cols_unique]], axis='columns')

cols_duplct_sorted = cols_duplct.sort_values()
halflength = len(cols_duplct_sorted)//2
cols_duplct_sorted_split_zipped = \
    zip(cols_duplct_sorted[:halflength], cols_duplct_sorted[halflength:])

for col_tuple in cols_duplct_sorted_split_zipped:
    df_ug_coal.loc[:, col_tuple[0]] = \
        df_ug_slc_flt[col_tuple[0]].combine_first(df_ug_slc_flt[col_tuple[1]])


# Shorten/simplify column names

df_cols_rename = \
    df_ug_coal.columns.str.extract(r'^(subject_id|exam_date)$|^scan.*_(.*)_completed$')
cols_rename = df_cols_rename[0].combine_first(df_cols_rename[1])

df_ug_coal.columns = cols_rename
df_ug_coal = df_ug_coal.apply(lambda col: col.apply(zero_to_nan))


# Write to CSV

local_file_dir = "."
local_file_path = local_file_dir + "/UMMAP_MRI_Scan_Report.csv"
df_ug_coal.to_csv(local_file_path, index=False)
