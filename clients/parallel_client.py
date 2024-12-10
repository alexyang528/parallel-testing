import requests
from typing import Dict, Optional, List
from dataclasses import dataclass


@dataclass
class ParallelConfig:
    """Configuration for the ParallelClient class"""
    base_url: str = "https://api.parallel.ai"
    api_key: str = ""
    task_id: str = ""
    runner_name: str = ""


class ParallelClient:
    """Generic client for interacting with Parallel AI's API"""
    
    def __init__(self, config: ParallelConfig):
        """Initialize ParallelClient with configuration"""
        self.config = config
        self._validate_config()
        
    def _validate_config(self) -> None:
        """Validate the basic configuration"""
        if not all([
            self.config.api_key,
            self.config.base_url,
            self.config.task_id,
            self.config.runner_name
        ]):
            raise ValueError("All configuration fields (api_key, base_url, task_id, runner_name) must be provided")

    def _get_headers(self, additional_headers: Optional[Dict] = None) -> Dict:
        """Get headers for API requests"""
        headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.config.api_key
        }
        if additional_headers:
            headers.update(additional_headers)
        return headers

    def _build_url(self, endpoint: str) -> str:
        """Build full URL for API request"""
        return f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def make_request(
        self,
        payload,
        task_id: Optional[str] = None,
        runner_name: Optional[str] = None,
        headers: Optional[Dict] = None,
    ) -> Dict:
        """
        Make a POST request to the Parallel API
        
        Args:
            task_id: Task ID (overrides config if provided)
            runner_name: Runner name (overrides config if provided)
            payload: Request payload/body
            headers: Additional headers
            
        Returns:
            API response as dictionary
        """
        task_id = task_id or self.config.task_id
        runner_name = runner_name or self.config.runner_name
        endpoint = f"v0/tasks/{task_id}/runners/{runner_name}/runs"
        
        url = self._build_url(endpoint)
        headers = self._get_headers(headers)
        arguments = {
            "arguments": payload
        }

        try:
            response = requests.post(
                url=url,
                headers=headers,
                json=arguments
            )
            response.raise_for_status()
            return response.json().get('output', {})
        except Exception as e:
            print(f"Error making request to {url}: {str(e)}")
            return {}


def create_parallel_client(
    api_key: str,
    task_id: str,
    runner_name: str = "krypton-80"
) -> ParallelClient:
    """
    Create a ParallelClient configured for a specific task
    
    Args:
        api_key: Parallel API key
        task_id: Task ID for the Parallel task
        runner_name: Runner name (defaults to krypton-80)
    """
    config = ParallelConfig(
        api_key=api_key,
        task_id=task_id,
        runner_name=runner_name
    )
    return ParallelClient(config)


def process_single_item(client: ParallelClient, data: Dict, arguments: Optional[List[str]] = None) -> Dict:
    """Process a single item through the Parallel API
    
    Args:
        client: ParallelClient instance
        data: Dictionary containing data
        arguments: Optional list of keys to use as API arguments. If None, uses entire data.
    
    Returns:
        Dictionary containing original row data merged with API response
    """
    # Filter row to only include specified arguments if provided
    if arguments:
        filtered_data = {k: data[k] if k in data else None for k in arguments}
        result = client.make_request(payload=filtered_data)
    else:
        result = client.make_request(payload=data)
    
    # Combine input data with API response
    return {**data, **result}
