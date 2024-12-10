import streamlit as st
import pandas as pd
import os
from datetime import datetime

def format_timestamp(filename):
    """Extract and format timestamp from results filename."""
    # Extract timestamp portion (assumes format results_YYYYMMDD_HHMMSS.csv)
    try:
        timestamp_str = filename.split('results_')[1].replace('.csv', '')
        timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
        return timestamp.strftime('%B %d, %Y at %I:%M:%S %p')
    except:
        return filename

def get_available_results():
    """Get all available results files organized by task ID."""
    results_path = "results"
    task_results = {}
    
    # Check if results directory exists
    if not os.path.exists(results_path):
        return {}
    
    # Get all task folders
    for task_id in os.listdir(results_path):
        task_dir = os.path.join(results_path, task_id)
        if os.path.isdir(task_dir):
            # Get all CSV files in the task directory
            csv_files = [f for f in os.listdir(task_dir) if f.endswith('.csv')]
            if csv_files:
                task_results[task_id] = csv_files
                
    return task_results

def main():
    st.title("W4 Forms Processing Results")
    
    # Get available results
    task_results = get_available_results()
    
    if not task_results:
        st.warning("No results found. Please run the processing script first.")
        return
    
    # Task ID selection
    selected_task = st.selectbox(
        "Select Task ID",
        options=list(task_results.keys()),
        format_func=lambda x: f"Task: {x}"
    )
    
    # File selection with formatted timestamps
    files = task_results[selected_task]
    file_options = {format_timestamp(f): f for f in files}
    selected_timestamp = st.selectbox(
        "Select Result",
        options=list(file_options.keys())
    )
    
    selected_file = file_options[selected_timestamp]
    file_path = os.path.join("results", selected_task, selected_file)
    
    # Load and display the selected CSV
    try:
        df = pd.read_csv(file_path)
        st.dataframe(df)
        
        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Results",
            data=csv,
            file_name=selected_file,
            mime='text/csv'
        )
        
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")

if __name__ == "__main__":
    main()