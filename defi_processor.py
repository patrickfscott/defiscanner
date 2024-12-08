# defi_processor.py
import requests
import json
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import sys

class DeFiChainDataProcessor:
    def __init__(self):
        self.base_url = 'https://api.llama.fi/overview/fees'

        # Configure logging
        self.logger = logging.getLogger('DefiProcessor')
        self.logger.setLevel(logging.INFO)
        
        # Create console handler with a higher log level
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        
        # Create formatter and add it to the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)
        
    def get_chain_names(self) -> List[str]:
        """Fetch and return list of all chain names from DefiLlama."""
        response = requests.get(f'{self.base_url}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees')
        data = response.json()
        return data["allChains"]
    
    def get_chain_data(self, chain_name: str) -> List[Dict]:
        """Fetch and process data for a specific chain."""
        response = requests.get(f'{self.base_url}/{chain_name}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyFees')
        data = response.json()

        
        # Process the time series data
        processed_data = []

        # If this is Ethereum, look for Tether/Circle protocols
        if chain_name.lower() == "ethereum":
            self.logger.info(f"Processing Ethereum data...")
            tether_circle_protocols = []
            
            for protocol in data.get("protocols", []):
                if protocol["name"].lower() in ["tether", "circle", "usdt", "usdc"]:
                    if "Ethereum" in protocol.get("chains", []):
                        self.logger.info(f"Found protocol: {protocol['name']}")
                        tether_circle_protocols.append(protocol)
            
            if not tether_circle_protocols:
                self.logger.warning("No Tether or Circle protocols found for Ethereum")
        
        for timestamp, value in data["totalDataChart"]:
            date = datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d')

            # For Ethereum, subtract any protocol fees at this timestamp
            if chain_name.lower() == "ethereum" and tether_circle_protocols:
                original_value = value
                for protocol in tether_circle_protocols:
                    # Look for matching timestamp in protocol breakdown
                    if "breakdown" in protocol:
                        for ts, fee in protocol["breakdown"]:
                            if ts == timestamp:
                                value -= fee
            
            processed_data.append({
                'date': date,
                'value': value
            })
        
        return processed_data

    def get_time_series_format(self) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
        """
        Return data in a format suitable for time series visualization.
        Returns:
            Tuple containing:
            - List of all dates
            - Dict of chain data where each chain contains a dict of date: value pairs
        """
        chain_names = self.get_chain_names()
        all_data = {}
        
        for chain in chain_names:
            try:
                chain_data = self.get_chain_data(chain)
                if chain_data:  # Only include chains with data
                    # Convert list of date/value dicts to date: value dict
                    all_data[chain] = {
                        entry['date']: entry['value']
                        for entry in chain_data
                    }
                    self.logger.info(f"Successfully processed {len(chain_data)} days of data for {chain}")
            except Exception as e:
                self.logger.error(f"Error fetching data for {chain}: {str(e)}")
                continue
        
        # Get all unique dates
        all_dates = sorted(set(
            date
            for chain_data in all_data.values()
            for date in chain_data.keys()
        ))
                
        return all_dates, all_data
