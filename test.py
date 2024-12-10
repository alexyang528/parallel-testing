#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime
from clients.parallel_client import create_parallel_client, process_single_item

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
        runner_name="krypton-80"
    )
    
    # Read the input CSV
    print("Reading W4 forms data...")
    df = pd.read_csv(f"data/{task_id}/{input_csv}")
    print(f"Found {len(df)} forms to process")

    # Process rows
    results = []
    for _, row in df.iterrows():
        payload = row.astype(str).to_dict()
        arguments = ['jurisdiction', 'agency_url', 'description', 'current_form_revision']

        result = process_single_item(client, payload, arguments)
        results.append(result)

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