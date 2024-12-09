import requests
import os
import pandas as pd
import time
from tqdm import tqdm

def make_api_call(row: dict, base_url: str, task_id: str, runner_name: str, api_key: str) -> dict:
    """Make HTTP POST request for a single row of data"""
    url = f"{base_url}/v0/tasks/{task_id}/runners/{runner_name}/runs"
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "arguments": {
            "jurisdiction": row['jurisdiction'],
            "agency_url": row['agency_url'],
            "description": row['description'],
            "current_form_revision": row['current_form_revision']
        }
    }
    
    # Make request
    response = requests.post(url=url, headers=headers, json=payload)
    result = response.json()
    
    try:
        output = result.get('output', {})
        return {
            'jurisdiction': row['jurisdiction'],
            'agency_url': row['agency_url'],
            'description': row['description'],
            'current_form_revision': row['current_form_revision'],
            'api_most_recent_form_revision': output.get('most_recent_form_revision'),
            'needs_update': output.get('needs_update'),
            'api_most_recent_form_pdf_url': output.get('most_recent_form_pdf_url'),
            'citations': output.get('citations'),
            'reasoning': output.get('reasoning')
        }
    except:
        print(f"Error encountered for {row['jurisdiction']}")
        return {
            'jurisdiction': row['jurisdiction'],
            'agency_url': row['agency_url'],
            'description': row['description'],
            'current_form_revision': row['current_form_revision'],
            'api_most_recent_form_revision': None,
            'needs_update': None,
            'api_most_recent_form_pdf_url': None,
            'citations': None,
            'reasoning': None
        }

if __name__ == "__main__":
    BASE_URL = "https://api.parallel.ai"
    TASK_ID = "4209efe0-fdfa-4ca9-b865-5f4de1e05b46"
    RUNNER_NAME = "krypton-80"
    API_KEY = os.getenv("PARALLEL_API_KEY")
    OUTPUT_FILE = 'w_4_forms_results.csv'
    
    df = pd.read_csv('w_4_forms.csv')
    results = []
    
    start_time = time.time()
    
    for idx, row in tqdm(df.iterrows(), total=len(df.index), desc="Processing forms"):
        result = make_api_call(row.astype(str).to_dict(), BASE_URL, TASK_ID, RUNNER_NAME, API_KEY)
        results.append(result)
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    
    elapsed_time = time.time() - start_time
    print(f"\nProcessing completed in {elapsed_time:.2f} seconds")
    print(f"Results saved to {OUTPUT_FILE}")