"""
Cost Basis Tracker
FIFO/LIFO accounting for crypto gains/losses
"""

import pandas as pd
from typing import Dict, List, Tuple
from collections import defaultdict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class CostBasisTracker:
    """Track cost basis and calculate realized/unrealized gains"""
    
    def __init__(self, method: str = 'FIFO'):
        self.method = method
        self.lots = defaultdict(list)  # {token: [lot_dict]}
        self.realized_gains = []
        self.disposals = []
        
        logger.info(f"CostBasisTracker initialized with {method} method")
    
    def calculate_cost_basis(
        self,
        transactions_df: pd.DataFrame,
        wallet_address: str
    ) -> Tuple[pd.DataFrame, 'CostBasisTracker']:
        """Calculate cost basis for all transactions"""
        
        if transactions_df.empty:
            return transactions_df, self
        
        logger.info(f"Calculating cost basis for {len(transactions_df)} transactions")
        
        # Sort by timestamp (oldest first for processing)
        sorted_df = transactions_df.sort_values('timestamp', ascending=True).copy()
        
        # Add cost basis columns
        sorted_df['realized_gain_loss'] = 0.0
        sorted_df['cost_basis_usd'] = 0.0
        
        acquisitions = 0
        disposals_count = 0
        
        for idx, tx in sorted_df.iterrows():
            token = tx['token_contract']
            quantity = tx['value_normalized']
            value_usd = tx.get('value_usd', 0)
            timestamp = tx['timestamp']
            tx_hash = tx['hash']
            direction = tx['direction']
            is_error = tx.get('is_error', False)
            
            if is_error or quantity == 0 or pd.isna(quantity):
                continue
            
            # ACQUISITIONS (inbound)
            if direction == 'inbound':
                cost_basis = value_usd if value_usd > 0 else 0
                self._add_acquisition(token, quantity, cost_basis, timestamp, tx_hash)
                sorted_df.at[idx, 'cost_basis_usd'] = cost_basis
                acquisitions += 1
            
            # DISPOSALS (outbound)
            elif direction == 'outbound':
                proceeds = value_usd if value_usd > 0 else 0
                disposal_record = self._process_disposal(token, quantity, proceeds, timestamp, tx_hash)
                
                sorted_df.at[idx, 'realized_gain_loss'] = disposal_record['realized_gain_loss']
                sorted_df.at[idx, 'cost_basis_usd'] = disposal_record['total_cost_basis']
                disposals_count += 1
        
        logger.info(f"Processed {acquisitions} acquisitions and {disposals_count} disposals")
        
        # Sort back to newest first
        final_df = sorted_df.sort_values('timestamp', ascending=False).reset_index(drop=True)
        
        return final_df, self
    
    def _add_acquisition(
        self,
        token: str,
        quantity: float,
        cost_basis_usd: float,
        timestamp: int,
        tx_hash: str
    ):
        """Record token acquisition"""
        
        if quantity <= 0:
            return
        
        cost_per_unit = cost_basis_usd / quantity if quantity > 0 else 0
        
        self.lots[token].append({
            'quantity': quantity,
            'cost_per_unit': cost_per_unit,
            'total_cost': cost_basis_usd,
            'timestamp': timestamp,
            'tx_hash': tx_hash,
            'acquisition_date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        })
    
    def _process_disposal(
        self,
        token: str,
        quantity: float,
        proceeds_usd: float,
        timestamp: int,
        tx_hash: str
    ) -> Dict:
        """Process disposal and calculate realized gains"""
        
        if quantity <= 0:
            return {
                'quantity_disposed': 0,
                'total_cost_basis': 0,
                'total_proceeds': 0,
                'realized_gain_loss': 0,
                'lots_used': []
            }
        
        # Handle tokens with no cost basis
        if token not in self.lots or not self.lots[token]:
            disposal_record = {
                'quantity_disposed': quantity,
                'total_cost_basis': 0,
                'total_proceeds': proceeds_usd,
                'realized_gain_loss': proceeds_usd,
                'lots_used': [],
                'note': 'zero_cost_basis',
                'timestamp': timestamp,
                'tx_hash': tx_hash,
                'disposal_date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'),
                'token': token
            }
            self.disposals.append(disposal_record)
            self.realized_gains.append(proceeds_usd)
            return disposal_record
        
        # Sort lots by method
        if self.method == 'FIFO':
            self.lots[token].sort(key=lambda x: x['timestamp'])
        else:  # LIFO
            self.lots[token].sort(key=lambda x: x['timestamp'], reverse=True)
        
        remaining_to_sell = quantity
        total_cost_basis = 0
        lots_used = []
        
        # Match disposal against lots
        while remaining_to_sell > 1e-10 and self.lots[token]:
            lot = self.lots[token][0]
            
            if lot['quantity'] <= remaining_to_sell:
                # Use entire lot
                quantity_from_lot = lot['quantity']
                cost_from_lot = lot['total_cost']
                remaining_to_sell -= quantity_from_lot
                total_cost_basis += cost_from_lot
                
                lots_used.append({
                    'quantity': quantity_from_lot,
                    'cost_basis': cost_from_lot,
                    'proceeds': (quantity_from_lot / quantity) * proceeds_usd,
                    'acquisition_date': lot['acquisition_date'],
                    'holding_period_days': (timestamp - lot['timestamp']) / 86400
                })
                
                self.lots[token].pop(0)
            else:
                # Use partial lot
                quantity_from_lot = remaining_to_sell
                cost_from_lot = remaining_to_sell * lot['cost_per_unit']
                total_cost_basis += cost_from_lot
                
                lots_used.append({
                    'quantity': quantity_from_lot,
                    'cost_basis': cost_from_lot,
                    'proceeds': (quantity_from_lot / quantity) * proceeds_usd,
                    'acquisition_date': lot['acquisition_date'],
                    'holding_period_days': (timestamp - lot['timestamp']) / 86400
                })
                
                # Update remaining lot
                lot['quantity'] -= quantity_from_lot
                lot['total_cost'] -= cost_from_lot
                
                remaining_to_sell = 0
        
        # Calculate realized gain/loss
        realized_gain_loss = proceeds_usd - total_cost_basis
        
        disposal_record = {
            'timestamp': timestamp,
            'tx_hash': tx_hash,
            'disposal_date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'),
            'token': token,
            'quantity_disposed': quantity,
            'total_proceeds': proceeds_usd,
            'total_cost_basis': total_cost_basis,
            'realized_gain_loss': realized_gain_loss,
            'lots_used': lots_used
        }
        
        self.disposals.append(disposal_record)
        self.realized_gains.append(realized_gain_loss)
        
        return disposal_record
    
    def get_total_realized_gains(self) -> float:
        """Get total realized gains/losses"""
        return sum(self.realized_gains)
    
    def get_total_unrealized_gains(self, current_prices: Dict[str, float] = None) -> float:
        """Get total unrealized gains/losses"""
        
        if not current_prices:
            return 0.0
        
        total_unrealized = 0
        
        for token, lots in self.lots.items():
            if not lots:
                continue
            
            total_quantity = sum(lot['quantity'] for lot in lots)
            total_cost = sum(lot['total_cost'] for lot in lots)
            current_price = current_prices.get(token, 0)
            
            if current_price > 0:
                current_value = total_quantity * current_price
                unrealized = current_value - total_cost
                total_unrealized += unrealized
        
        return total_unrealized
    
    def get_realized_gains_for_period(
        self,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Get realized gains for specific period"""
        
        if not self.disposals:
            return pd.DataFrame()
        
        period_disposals = [
            d for d in self.disposals
            if start_date <= d['disposal_date'] <= end_date
        ]
        
        if not period_disposals:
            return pd.DataFrame()
        
        return pd.DataFrame(period_disposals)
    
    def get_unrealized_positions(self, current_prices: Dict[str, float]) -> pd.DataFrame:
        """Get current unrealized positions"""
        
        positions = []
        
        for token, lots in self.lots.items():
            if not lots:
                continue
            
            total_quantity = sum(lot['quantity'] for lot in lots)
            total_cost = sum(lot['total_cost'] for lot in lots)
            avg_cost_per_unit = total_cost / total_quantity if total_quantity > 0 else 0
            
            current_price = current_prices.get(token, 0)
            current_value = total_quantity * current_price
            unrealized_gain_loss = current_value - total_cost
            
            positions.append({
                'token': token,
                'quantity': total_quantity,
                'total_cost_basis': total_cost,
                'avg_cost_per_unit': avg_cost_per_unit,
                'current_price': current_price,
                'current_value': current_value,
                'unrealized_gain_loss': unrealized_gain_loss,
                'return_pct': (unrealized_gain_loss / total_cost * 100) if total_cost > 0 else 0
            })
        
        return pd.DataFrame(positions)