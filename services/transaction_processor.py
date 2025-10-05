"""
Transaction_processor.py
Transaction Processor
Enhances transactions with metadata, pricing, and classification
"""

import pandas as pd
from typing import Dict, Set
import logging

from services.cache_manager import CacheManager
from services.blockchain_clients import BlockchainClientManager

logger = logging.getLogger(__name__)


# Major token metadata (hardcoded for speed)
MAJOR_TOKENS = {
    '0x0000000000000000000000000000000000000000': {
        'symbol': 'ETH', 'name': 'Ethereum', 'decimals': 18, 'is_stablecoin': False
    },
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': {
        'symbol': 'WETH', 'name': 'Wrapped Ether', 'decimals': 18, 'is_stablecoin': False
    },
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': {
        'symbol': 'USDC', 'name': 'USD Coin', 'decimals': 6, 'is_stablecoin': True
    },
    '0xdac17f958d2ee523a2206206994597c13d831ec7': {
        'symbol': 'USDT', 'name': 'Tether USD', 'decimals': 6, 'is_stablecoin': True
    },
    '0x6b175474e89094c44da98b954eedeac495271d0f': {
        'symbol': 'DAI', 'name': 'Dai Stablecoin', 'decimals': 18, 'is_stablecoin': True
    }
}


class TransactionProcessor:
    """Processes and enhances transaction data"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache = cache_manager
        self.blockchain_clients = BlockchainClientManager()
        logger.info("TransactionProcessor initialized")
    
    async def enhance_transactions(
        self,
        transactions_df: pd.DataFrame,
        wallet_address: str
    ) -> pd.DataFrame:
        """Enhance transactions with metadata and pricing"""
        
        if transactions_df.empty:
            return transactions_df
        
        logger.info(f"Enhancing {len(transactions_df)} transactions")
        
        # Step 1: Add token metadata
        enhanced_df = await self._add_token_metadata(transactions_df)
        
        # Step 2: Add historical prices
        enhanced_df = await self._add_historical_prices(enhanced_df)
        
        # Step 3: Calculate USD values
        enhanced_df = self._calculate_usd_values(enhanced_df)
        
        logger.info(f"Enhancement complete for {len(enhanced_df)} transactions")
        return enhanced_df
    
    async def _add_token_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add token metadata to transactions"""
        
        # Get unique token contracts
        unique_contracts = set(df['token_contract'].dropna().unique())
        unique_contracts = [c for c in unique_contracts if c and len(c) > 10]
        
        logger.info(f"Fetching metadata for {len(unique_contracts)} unique tokens")
        
        # Check cache first
        metadata_map = {}
        contracts_to_fetch = []
        
        for contract in unique_contracts:
            if contract in MAJOR_TOKENS:
                metadata_map[contract] = MAJOR_TOKENS[contract]
            else:
                cached_meta = self.cache.get_token_metadata(contract)
                if cached_meta:
                    metadata_map[contract] = cached_meta
                else:
                    contracts_to_fetch.append(contract)
        
        # Fetch missing metadata
        if contracts_to_fetch:
            logger.info(f"Fetching {len(contracts_to_fetch)} missing token metadata")
            new_metadata = await self.blockchain_clients.get_token_metadata(contracts_to_fetch)
            
            for contract, meta in new_metadata.items():
                metadata_map[contract] = meta
                self.cache.set_token_metadata(contract, meta)
        
        # Apply metadata to dataframe
        enhanced_df = df.copy()
        
        for idx, row in enhanced_df.iterrows():
            contract = row.get('token_contract', '').lower()
            if contract in metadata_map:
                meta = metadata_map[contract]
                enhanced_df.at[idx, 'token_symbol'] = meta.get('symbol', 'UNKNOWN')
                enhanced_df.at[idx, 'token_name'] = meta.get('name', 'Unknown')
                enhanced_df.at[idx, 'token_decimals'] = meta.get('decimals', 18)
                enhanced_df.at[idx, 'is_stablecoin'] = meta.get('is_stablecoin', False)
        
        return enhanced_df
    
    async def _add_historical_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add historical prices to transactions"""
        
        logger.info("Fetching historical prices")
        
        enhanced_df = df.copy()
        enhanced_df['price_usd'] = 0.0
        enhanced_df['price_source'] = ''
        
        # Process in batches
        batch_size = 50
        for i in range(0, len(enhanced_df), batch_size):
            batch = enhanced_df.iloc[i:i+batch_size]
            
            for idx in batch.index:
                row = enhanced_df.loc[idx]
                contract = row.get('token_contract', '').lower()
                timestamp = row.get('timestamp', 0)
                is_stablecoin = row.get('is_stablecoin', False)
                
                # Stablecoins = $1.00
                if is_stablecoin:
                    enhanced_df.at[idx, 'price_usd'] = 1.0
                    enhanced_df.at[idx, 'price_source'] = 'stablecoin'
                    continue
                
                # Check cache
                cached_price = self.cache.get_historical_price(contract, timestamp)
                if cached_price:
                    enhanced_df.at[idx, 'price_usd'] = cached_price.get('price', 0)
                    enhanced_df.at[idx, 'price_source'] = cached_price.get('source', 'cache')
                    continue
                
                # Fetch price
                price = await self.blockchain_clients.get_historical_price(contract, timestamp)
                if price and price > 0:
                    price_data = {'price': price, 'source': 'moralis', 'timestamp': timestamp}
                    self.cache.set_historical_price(contract, timestamp, price_data)
                    enhanced_df.at[idx, 'price_usd'] = price
                    enhanced_df.at[idx, 'price_source'] = 'moralis'
        
        logger.info("Historical pricing complete")
        return enhanced_df
    
    def _calculate_usd_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate USD values for all transactions"""
        
        enhanced_df = df.copy()
        enhanced_df['value_usd'] = 0.0
        enhanced_df['gas_fee_usd'] = 0.0
        
        for idx, row in enhanced_df.iterrows():
            value_normalized = row.get('value_normalized', 0)
            price_usd = row.get('price_usd', 0)
            gas_fee_eth = row.get('gas_fee_eth', 0)
            
            # Calculate value in USD
            if value_normalized and price_usd:
                enhanced_df.at[idx, 'value_usd'] = value_normalized * price_usd
            
            # Calculate gas fee in USD (always ETH)
            if gas_fee_eth > 0:
                # Get ETH price for this timestamp
                eth_price = row.get('price_usd', 0) if row.get('token_symbol') == 'ETH' else 0
                if eth_price > 0:
                    enhanced_df.at[idx, 'gas_fee_usd'] = gas_fee_eth * eth_price
        
        return enhanced_df
    
    def classify_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify transactions into accounting categories"""
        
        if df.empty:
            return df
        
        logger.info(f"Classifying {len(df)} transactions")
        
        classified_df = df.copy()
        
        # Initialize classification columns
        classified_df['transaction_category'] = ''
        classified_df['income_type'] = ''
        classified_df['expense_type'] = ''
        classified_df['is_income'] = False
        classified_df['is_expense'] = False
        classified_df['is_transfer'] = False
        classified_df['accounting_treatment'] = ''
        
        for idx, row in classified_df.iterrows():
            classification = self._classify_single_transaction(row)
            
            for key, value in classification.items():
                classified_df.at[idx, key] = value
        
        logger.info("Classification complete")
        return classified_df
    
    def _classify_single_transaction(self, tx: pd.Series) -> Dict:
        """Classify a single transaction"""
        
        direction = tx.get('direction', '')
        value_usd = tx.get('value_usd', 0)
        token_symbol = tx.get('token_symbol', '')
        
        classification = {
            'transaction_category': 'transfer',
            'income_type': '',
            'expense_type': '',
            'is_income': False,
            'is_expense': False,
            'is_transfer': False,
            'accounting_treatment': 'balance_sheet'
        }
        
        # Determine if income
        if direction == 'inbound' and value_usd > 0:
            # Check if it looks like a reward/airdrop (from zero address or unknown)
            from_addr = tx.get('from_address', '')
            if from_addr == '0x0000000000000000000000000000000000000000':
                classification['income_type'] = 'mining_reward'
                classification['is_income'] = True
                classification['transaction_category'] = 'income'
                classification['accounting_treatment'] = 'income'
            else:
                # Regular transfer in
                classification['is_transfer'] = True
                classification['transaction_category'] = 'transfer'
        
        # Determine if expense
        elif direction == 'outbound' and value_usd > 0:
            classification['is_transfer'] = True
            classification['transaction_category'] = 'transfer'
            
            # If significant gas fees, might be a swap or DeFi interaction
            gas_fee = tx.get('gas_fee_usd', 0)
            if gas_fee > 10:
                classification['expense_type'] = 'protocol_fee'
                classification['is_expense'] = True
                classification['transaction_category'] = 'defi_interaction'
                classification['accounting_treatment'] = 'expense'
        
        return classification