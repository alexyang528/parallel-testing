#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from clients.parallel_client import create_parallel_client, process_single_item

def process_row_with_lock(args):
    """
    Process a single row with thread-safe progress updates
    """
    client, row, arguments, progress_lock, pbar = args
    
    try:
        payload = row.astype(str).to_dict()
        result = process_single_item(client, payload, arguments)
        
        # Update progress bar in a thread-safe manner
        with progress_lock:
            pbar.update(1)
            
        return result
    except Exception as e:
        # Handle any errors that occur during processing
        with progress_lock:
            print(f"Error processing row: {e}")
        return None

def main():
    # Get API key from environment variable
    api_key = os.environ.get('PARALLEL_API_KEY')
    if not api_key:
        print("Error: PARALLEL_API_KEY environment variable not set")
        return
    
    # Configure task
    task_id = "4209efe0-fdfa-4ca9-b865-5f4de1e05b46"
    input_csv = "w_4_forms.csv"
    
    # Create the Parallel client
    client = create_parallel_client(
        api_key=api_key,
        task_id=task_id,
        runner_name="mercury"
    )
    
    # Read the input CSV
    df = pd.read_csv(f"data/{task_id}/{input_csv}")
    
    # Create a lock for thread-safe progress bar updates
    progress_lock = Lock()
    
    # Initialize progress bar
    pbar = tqdm(total=len(df), desc="Processing forms")
    
    # Define arguments for form processing
    arguments = ['jurisdiction', 'agency_url', 'description', 'current_form_revision']
    
    # Process rows in parallel using ThreadPoolExecutor
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Create a list of arguments for each row
        process_args = [
            (client, row, arguments, progress_lock, pbar)
            for _, row in df.iterrows()
        ]
        
        # Execute processing in parallel
        results = list(executor.map(process_row_with_lock, process_args))
    
    # Close progress bar
    pbar.close()
    
    # Filter out None results (failed processing)
    results = [r for r in results if r is not None]
    
    # Create results directory with task ID if it doesn't exist
    results_dir = os.path.join("results", task_id)
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate timestamp and create filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results_{timestamp}.csv"
    
    # Save results with timestamp in task-specific directory
    output_path = os.path.join(results_dir, filename)
    pd.DataFrame(results).to_csv(output_path, index=False)
    print(f"Results saved to: {output_path}")
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    main()