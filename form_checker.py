# form_checker.py
import requests
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
from tqdm import tqdm
import time


@dataclass
class FormCheckerConfig:
    """Configuration for the FormChecker class"""
    base_url: str = "https://api.parallel.ai"
    task_id: str = "4209efe0-fdfa-4ca9-b865-5f4de1e05b46"
    runner_name: str = "krypton-80"
    api_key: str = ""


class FormChecker:
    """Class to handle form checking operations via API"""
    
    def __init__(self, config: FormCheckerConfig):
        """Initialize FormChecker with configuration"""
        self.config = config
        self._validate_config()
        
    def _validate_config(self) -> None:
        """Validate the configuration"""
        if not self.config.api_key:
            raise ValueError("API key is required")
        if not all([self.config.base_url, self.config.task_id, self.config.runner_name]):
            raise ValueError("All configuration fields must be provided")

    def make_api_call(self, row: Dict) -> Dict:
        """Make HTTP POST request for a single row of data"""
        url = f"{self.config.base_url}/v0/tasks/{self.config.task_id}/runners/{self.config.runner_name}/runs"
        
        headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.config.api_key
        }
        
        payload = {
            "arguments": {
                "jurisdiction": row['jurisdiction'],
                "agency_url": row['agency_url'],
                "description": row['description'],
                "current_form_revision": row['current_form_revision']
            }
        }
        
        try:
            response = requests.post(url=url, headers=headers, json=payload)
            result = response.json()
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
        except Exception as e:
            print(f"Error encountered for {row['jurisdiction']}: {str(e)}")
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

    def process_forms(self, input_data: pd.DataFrame, output_file: Optional[str] = None) -> pd.DataFrame:
        """
        Process forms data through the API
        
        Args:
            input_data: DataFrame containing form data
            output_file: Optional path to save results incrementally
            
        Returns:
            DataFrame containing results
        """
        results = []
        start_time = time.time()
        
        for idx, row in tqdm(input_data.iterrows(), total=len(input_data.index), desc="Processing forms"):
            result = self.make_api_call(row.astype(str).to_dict())
            results.append(result)
            
            if output_file:
                pd.DataFrame(results).to_csv(output_file, index=False)
        
        elapsed_time = time.time() - start_time
        print(f"\nProcessing completed in {elapsed_time:.2f} seconds")
        
        if output_file:
            print(f"Results saved to {output_file}")
            
        return pd.DataFrame(results)