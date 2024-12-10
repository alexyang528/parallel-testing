#!/usr/bin/env python3
import os
import pandas as pd
from clients.parallel_client import create_parallel_client, process_single_item

def main():
    # Get API key from environment variable
    api_key = os.environ.get('PARALLEL_API_KEY')
    if not api_key:
        print("Error: PARALLEL_API_KEY environment variable not set")
        return
    
    # Configure task
    task_id = "4209efe0-fdfa-4ca9-b865-5f4de1e05b46"
    
    # Create the Parallel client
    client = create_parallel_client(
        api_key=api_key,
        task_id=task_id,
        runner_name="krypton-80"
    )
    
    # Read the input CSV
    print("Reading W4 forms data...")
    df = pd.read_csv("data/w_4_forms.csv")
    print(f"Found {len(df)} forms to process")

    # Process rows
    results = []
    for _, row in df.iterrows():
        payload = row.astype(str).to_dict()
        allowed_keys = {'jurisdiction', 'agency_url', 'description', 'current_form_revision'}
        filtered_payload = {k: payload[k] for k in allowed_keys}


        print(payload)
        result = process_single_item(client, filtered_payload)
        results.append(result)
        break

    pd.DataFrame(results).to_csv("results/results.csv", index=False)
        
    return pd.DataFrame(results)


if __name__ == "__main__":
    main()
