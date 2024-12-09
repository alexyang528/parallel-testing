# app.py
import streamlit as st
import pandas as pd
import os
from form_checker import FormChecker, FormCheckerConfig

st.title("Form Checker")
api_key = st.secrets["api_key"]
results = []
results_df = pd.read_csv("w_4_forms_results.csv")

if st.button("Process Forms"):
    # Read the CSV
    input_df = pd.read_csv('w_4_forms.csv')
    
    # Initialize form checker
    config = FormCheckerConfig(api_key=api_key)
    checker = FormChecker(config)
    
    # Process forms with progress bar
    progress_bar = st.progress(0.0, text="Processing form updates...")
    
    for idx, row in input_df.iterrows():
        result = checker.make_api_call(row)
        results.append(result)
        progress_bar.progress(idx / len(input_df.index), text="Processing form updates...")
        print(f"{idx} of {len(input_df.index)} completed.")
    
    results_df = pd.DataFrame(results)
    results_df.to_csv("w_4_forms_results.csv")
    
# Display results
st.dataframe(results_df)

# Download button
st.download_button(
    "Download Results",
    results_df.to_csv(index=False),
    "form_checker_results.csv",
    "text/csv"
)