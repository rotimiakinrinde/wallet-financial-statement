"""
Blockchain API Clients
Manages connections to Etherscan, Moralis, CoinGecko
"""

import os
import aiohttp
import asyncio
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime
import logging


logger = logging.getLogger(__name__)

print("=" * 50)
print("DEBUG: API KEYS CHECK")
print(f"Etherscan key loaded: {os.getenv('ETHERSCAN_API_KEY') is not None}")
print(f"Moralis key loaded: {os.getenv('MORALIS_API_KEY') is not None}")
if os.getenv('ETHERSCAN_API_KEY'):
    print(f"Etherscan key preview: {os.getenv('ETHERSCAN_API_KEY')[:10]}...")
if os.getenv('MORALIS_API_KEY'):
    print(f"Moralis key preview: {os.getenv('MORALIS_API_KEY')[:10]}...")
print("=" * 50)

class BlockchainClientManager:
    """Manages all blockchain API clients"""
    
    def __init__(self):
        self.etherscan_key = os.getenv('ETHERSCAN_API_KEY')
        self.moralis_key = os.getenv('MORALIS_API_KEY')
        self.coingecko_key = os.getenv('COINGECKO_API_KEY')
        
        self.etherscan_base = "https://api.etherscan.io/v2/api"
        self.moralis_base = "https://deep-index.moralis.io/api/v2.2"
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        
        logger.info("BlockchainClientManager initialized")
    
    async def fetch_etherscan_transactions(self, wallet_address: str) -> pd.DataFrame:
        """Fetch all transaction types from Etherscan V2"""
        
        all_txs = []
        
        async with aiohttp.ClientSession() as session:
            # Fetch all transaction types in parallel
            tasks = [
                self._fetch_etherscan_tx_type(session, wallet_address, 'normal'),
                self._fetch_etherscan_tx_type(session, wallet_address, 'internal'),
                self._fetch_etherscan_tx_type(session, wallet_address, 'erc20')
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for tx_type, result in zip(['normal', 'internal', 'erc20'], results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch {tx_type} transactions: {result}")
                    continue
                
                for tx in result:
                    normalized = self._normalize_etherscan_tx(tx, tx_type, wallet_address)
                    if normalized:
                        all_txs.append(normalized)
        
        if all_txs:
            df = pd.DataFrame(all_txs)
            df = df.sort_values('timestamp', ascending=False).reset_index(drop=True)
            logger.info(f"Fetched {len(df)} Etherscan transactions")
            return df
        
        return pd.DataFrame()
    
    async def _fetch_etherscan_tx_type(
        self,
        session: aiohttp.ClientSession,
        wallet_address: str,
        tx_type: str
    ) -> List[Dict]:
        """Fetch specific transaction type from Etherscan"""
        
        action_map = {
            'normal': 'txlist',
            'internal': 'txlistinternal',
            'erc20': 'tokentx'
        }
        
        params = {
            'chainid': '1',
            'module': 'account',
            'action': action_map[tx_type],
            'address': wallet_address,
            'startblock': '0',
            'endblock': '99999999',
            'page': '1',
            'offset': '10000',
            'sort': 'desc',
            'apikey': self.etherscan_key
        }
        
        try:
            async with session.get(self.etherscan_base, params=params, timeout=30) as response:
                data = await response.json()
                
                if data.get('status') == '1':
                    return data.get('result', [])
                elif data.get('message') == 'No transactions found':
                    return []
                else:
                    logger.warning(f"Etherscan warning for {tx_type}: {data.get('message')}")
                    return []
                    
        except Exception as e:
            logger.error(f"Etherscan request failed for {tx_type}: {e}")
            return []
    
    def _normalize_etherscan_tx(self, tx: Dict, tx_type: str, wallet_address: str) -> Optional[Dict]:
        """Normalize Etherscan transaction format"""
        
        try:
            base_tx = {
                'hash': tx.get('hash', ''),
                'timestamp': int(tx.get('timeStamp', 0)),
                'block_number': int(tx.get('blockNumber', 0)),
                'from_address': tx.get('from', '').lower(),
                'to_address': tx.get('to', '').lower(),
                'wallet_address': wallet_address.lower(),
                'source': 'etherscan',
                'transaction_type': tx_type,
                'is_error': tx.get('isError', '0') == '1',
                'gas_used': int(tx.get('gasUsed', 0)),
                'gas_price': int(tx.get('gasPrice', 0))
            }
            
            # Direction
            wallet_lower = wallet_address.lower()
            if base_tx['from_address'] == wallet_lower:
                base_tx['direction'] = 'outbound'
            elif base_tx['to_address'] == wallet_lower:
                base_tx['direction'] = 'inbound'
            else:
                base_tx['direction'] = 'internal'
            
            # Type-specific fields
            if tx_type == 'normal':
                value_wei = int(tx.get('value', 0))
                base_tx.update({
                    'token_contract': '0x0000000000000000000000000000000000000000',
                    'token_symbol': 'ETH',
                    'token_decimals': 18,
                    'value_wei': value_wei,
                    'value_normalized': value_wei / 10**18,
                    'gas_fee_wei': base_tx['gas_used'] * base_tx['gas_price'],
                    'gas_fee_eth': (base_tx['gas_used'] * base_tx['gas_price']) / 10**18
                })
                
            elif tx_type == 'erc20':
                decimals = int(tx.get('tokenDecimal', 18))
                value_wei = int(tx.get('value', 0))
                base_tx.update({
                    'token_contract': tx.get('contractAddress', '').lower(),
                    'token_symbol': tx.get('tokenSymbol', 'UNKNOWN'),
                    'token_name': tx.get('tokenName', ''),
                    'token_decimals': decimals,
                    'value_wei': value_wei,
                    'value_normalized': value_wei / (10 ** decimals),
                    'gas_fee_wei': base_tx['gas_used'] * base_tx['gas_price'],
                    'gas_fee_eth': (base_tx['gas_used'] * base_tx['gas_price']) / 10**18
                })
                
            elif tx_type == 'internal':
                value_wei = int(tx.get('value', 0))
                base_tx.update({
                    'token_contract': '0x0000000000000000000000000000000000000000',
                    'token_symbol': 'ETH',
                    'token_decimals': 18,
                    'value_wei': value_wei,
                    'value_normalized': value_wei / 10**18,
                    'gas_fee_wei': 0,
                    'gas_fee_eth': 0
                })
            
            # Add date
            base_tx['date'] = datetime.fromtimestamp(base_tx['timestamp']).strftime('%Y-%m-%d')
            base_tx['datetime'] = datetime.fromtimestamp(base_tx['timestamp']).isoformat()
            
            return base_tx
            
        except Exception as e:
            logger.error(f"Error normalizing transaction {tx.get('hash', 'unknown')}: {e}")
            return None
    
    async def fetch_moralis_transactions(self, wallet_address: str) -> pd.DataFrame:
        """Fetch transactions from Moralis"""
        
        all_txs = []
        
        async with aiohttp.ClientSession() as session:
            headers = {'X-API-Key': self.moralis_key, 'Accept': 'application/json'}
            endpoint = f"{self.moralis_base}/{wallet_address}"
            params = {'chain': 'eth', 'limit': 100}
            
            cursor = None
            page = 0
            max_pages = 10
            
            while page < max_pages:
                if cursor:
                    params['cursor'] = cursor
                
                try:
                    async with session.get(endpoint, params=params, headers=headers, timeout=30) as response:
                        if response.status != 200:
                            break
                        
                        data = await response.json()
                        transactions = data.get('result', [])
                        
                        if not transactions:
                            break
                        
                        for tx in transactions:
                            processed = self._normalize_moralis_tx(tx, wallet_address)
                            if processed:
                                all_txs.append(processed)
                        
                        cursor = data.get('cursor')
                        if not cursor:
                            break
                        
                        page += 1
                        await asyncio.sleep(1.2)
                        
                except Exception as e:
                    logger.error(f"Moralis request failed: {e}")
                    break
        
        if all_txs:
            df = pd.DataFrame(all_txs)
            df = df.sort_values('timestamp', ascending=False).reset_index(drop=True)
            logger.info(f"Fetched {len(df)} Moralis transactions")
            return df
        
        return pd.DataFrame()
    
    def _normalize_moralis_tx(self, tx: Dict, wallet_address: str) -> Optional[Dict]:
        """Normalize Moralis transaction format"""
        
        try:
            block_timestamp = tx.get('block_timestamp', '')
            if 'T' in block_timestamp:
                timestamp = int(datetime.fromisoformat(block_timestamp.replace('Z', '+00:00')).timestamp())
            else:
                timestamp = int(block_timestamp) if block_timestamp else 0
            
            return {
                'hash': tx.get('hash', ''),
                'timestamp': timestamp,
                'block_number': int(tx.get('block_number', 0)),
                'from_address': tx.get('from_address', '').lower(),
                'to_address': tx.get('to_address', '').lower() if tx.get('to_address') else '',
                'wallet_address': wallet_address.lower(),
                'source': 'moralis',
                'value_eth': float(tx.get('value', 0)) / 10**18,
                'gas_fee_eth': float(tx.get('transaction_fee', 0)) / 10**18,
                'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'),
                'datetime': datetime.fromtimestamp(timestamp).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing Moralis transaction: {e}")
            return None
    
    async def get_token_metadata(self, contract_addresses: List[str]) -> Dict:
        """Get token metadata from Moralis"""
        
        async with aiohttp.ClientSession() as session:
            headers = {'X-API-Key': self.moralis_key}
            endpoint = f"{self.moralis_base}/erc20/metadata"
            
            metadata_results = {}
            
            # Process in batches of 25
            for i in range(0, len(contract_addresses), 25):
                batch = contract_addresses[i:i+25]
                params = {'chain': 'eth', 'addresses': batch}
                
                try:
                    async with session.get(endpoint, params=params, headers=headers, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            for token in data:
                                contract = token.get('address', '').lower()
                                metadata_results[contract] = {
                                    'symbol': token.get('symbol', 'UNKNOWN'),
                                    'name': token.get('name', 'Unknown Token'),
                                    'decimals': int(token.get('decimals', 18)),
                                    'is_stablecoin': token.get('symbol', '').upper() in ['USDC', 'USDT', 'DAI', 'FRAX']
                                }
                    
                    await asyncio.sleep(1.2)
                    
                except Exception as e:
                    logger.error(f"Token metadata fetch failed: {e}")
            
            return metadata_results
    
    async def get_historical_price(
        self,
        contract_address: str,
        timestamp: int
    ) -> Optional[float]:
        """Get historical price from Moralis"""
        
        if contract_address == '0x0000000000000000000000000000000000000000':
            contract_address = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'  # WETH
        
        async with aiohttp.ClientSession() as session:
            headers = {'X-API-Key': self.moralis_key}
            endpoint = f"{self.moralis_base}/erc20/{contract_address}/price"
            params = {'chain': 'eth', 'to_date': timestamp}
            
            try:
                async with session.get(endpoint, params=params, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data.get('usdPrice', 0))
            except Exception as e:
                logger.error(f"Price fetch failed for {contract_address}: {e}")
        
        return None