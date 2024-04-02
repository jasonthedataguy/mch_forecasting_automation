from flask import Flask, render_template, request, session, send_file, send_from_directory, redirect, url_for
# for data processing
import pandas as pd
import numpy as np
import glob 
import os 
import datetime
from datetime import date, datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

from static.etl_process import *

# set up api call bigquery
key_path = 'api_key.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        uploaded_files = request.files.getlist("file")  # Get multiple files
        if len(uploaded_files) == 4:  # Ensure all files are uploaded
            try:
                df_contribution = pd.read_csv(uploaded_files[0])
                df_region_contribution = pd.read_csv(uploaded_files[1])
                df_region_sub_manual = pd.read_csv(uploaded_files[2])
                df_muf_input = pd.read_csv(uploaded_files[3])
                # ... (process data using Pandas)
                # collect data
                df_master, df_so, df_si, df_master_calendar, df_contribution_melted, df_region_contribution_melted, df_region_list, df_price_list, df_master_date, df_codeMappingMaster, df_mappingItemCode = etl_collect_clean(df_contribution, df_region_contribution)
                # baseline
                df_baseline = etl_baseline(df_so, df_master, df_master_calendar)
                df_groupSKU_master = etl_groupSKU_master(df_so, df_master)
                df_region_groupSKU = etl_regionContribution_groupSKU(df_groupSKU_master, df_region_sub_manual)
                df_region_subDivision = etl_regionContribution_subDiv(df_groupSKU_master, df_region_sub_manual)
                # manual & default groupSKU
                df_manual_groupSKU = etl_manual_groupSKU(df_contribution_melted, df_region_contribution_melted, df_region_groupSKU, df_region_subDivision)
                df_default_groupSKU =  etl_default_groupSKU(df_groupSKU_master, df_contribution_melted)
                # price
                df_price_list_month = etl_price(df_price_list, df_master_date)
                # MUF
                df_MUF = etl_muf_data(df_muf_input, df_manual_groupSKU, df_default_groupSKU, df_price_list_month)
                # DC contribution
                df_DC_contribution = etl_DC_contribution(df_si, df_master)
                df_DC_contribution_by_DPName = etl_DC_contribution_by_DPName(df_DC_contribution)
                df_DC_contribution_by_groupSKU = etl_DC_contribution_by_groupSKU(df_DC_contribution)
                df_DC_contribution_by_subDivision = etl_DC_contribution_by_subDivision(df_DC_contribution)
                df_MUF_withDC = etl_MUF_withDC(df_master_date, df_MUF, df_DC_contribution_by_DPName, df_DC_contribution_by_groupSKU, df_DC_contribution_by_subDivision)
                # Weekly Phasing
                df_weeklyPhasing = etl_WeeklyPhasing(df_MUF_withDC, df_master_date, df_master, df_codeMappingMaster, df_mappingItemCode)

                # downloadable_files = {
                #     "baseline.csv": df_baseline,
                #     "default_contribution.csv": df_default_groupSKU,
                #     "manual_contribution.csv": df_manual_groupSKU,
                #     "muf_input.csv": df_muf_input,
                #     "weekly_phasing.csv": df_weeklyPhasing
                # }

                df_baseline.to_csv('output files/df_baseline.csv', index = False)
                df_default_groupSKU.to_csv('output files/df_default_groupSKU.csv', index = False)
                df_manual_groupSKU.to_csv('output files/df_manual_groupSKU.csv', index = False)
                df_muf_input.to_csv('output files/df_muf_input.csv', index = False)
                df_weeklyPhasing.to_csv('output files/df_weeklyPhasing.csv', index = False)

                table_id_df_baseline = 'mch-dwh-409503.MCH_Output.df_baseline'
                table_id_df_region_contribution = 'mch-dwh-409503.MCH_Output.df_region_contribution'
                job_baseline = client.load_table_from_dataframe(
                    df_baseline, table_id_df_baseline
                )
                job_region_contribution = client.load_table_from_dataframe(
                    df_region_contribution, table_id_df_region_contribution
                )

                return redirect(url_for('download'))
            except Exception as e:
                return render_template("error.html", error_message=f"Error reading CSV file: {e}")
    return render_template("upload.html")

local_file_path = "output files"  

@app.route("/download/", methods=["GET"])
def download():
    return render_template('download.html', files = os.listdir(local_file_path))



@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    return send_from_directory(local_file_path, filename)



# @app.route("/download/<filename>", methods=["GET"])
# def download_file(filename, downloadable_files):               
    # df = downloadable_files.get(filename)
    # if df is not None:
    #     return send_file(df.to_csv(), mimetype="text/csv", as_attachment=True, attachment_filename=filename)
    # else:
    #     return "Invalid filename", 404

if __name__ == "__main__":
    app.run(debug=True)  # Enable debug mode for development
