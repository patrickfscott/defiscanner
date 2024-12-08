# defi_processor.py
import requests
import json
from datetime import datetime
from typing import Dict, List, Tuple

class DeFiChainDataProcessor:
    def __init__(self):
        self.base_url = 'https://api.llama.fi/overview/fees'
        
    def get_chain_names(self) -> List[str]:
        """Fetch and return list of all chain names from DefiLlama."""
        response = requests.get(f'{self.base_url}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees')
        data = response.json()
        return data["allChains"]

    def get_protocol_data_for_ethereum(self) -> Dict[str, float]:
        """Fetch protocol-level data for Ethereum chain."""
        response = requests.get(f'{self.base_url}/Ethereum?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=false&dataType=dailyFees')
        data = response.json()
        
        # Get daily fees for Tether and Circle from protocols array
        protocol_fees = {}
        for protocol in data.get("protocols", []):
            if protocol["name"].lower() in ["tether", "circle", "usdt", "usdc"]:
                # Check if protocol operates on Ethereum
                if "Ethereum" in protocol.get("chains", []):
                    protocol_fees[protocol["name"]] = protocol.get("total24h", 0)
        
        return protocol_fees
    
    def get_chain_data(self, chain_name: str) -> List[Dict]:
        """Fetch and process data for a specific chain."""
        response = requests.get(f'{self.base_url}/{chain_name}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyFees')
        data = response.json()

        # If this is Ethereum, get protocol data to subtract
        protocol_fees = {}
        if chain_name.lower() == "ethereum":
            protocol_data = self.get_protocol_data_for_ethereum()
        
        # Process the time series data
        processed_data = []
        for timestamp, value in data["totalDataChart"]:
            date = datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d')

            # For Ethereum, subtract Tether and Circle fees
            if chain_name.lower() == "ethereum":
                for protocol_values in protocol_data.values():
                    if date in protocol_values:
                        value -= protocol_values[date]
            
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
            except Exception as e:
                print(f"Error fetching data for {chain}: {str(e)}")
                continue
        
        # Get all unique dates
        all_dates = sorted(set(
            date
            for chain_data in all_data.values()
            for date in chain_data.keys()
        ))
                
        return all_dates, all_data
