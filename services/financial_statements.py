"""
financial_statements.py
Financial Statements Generator
Creates Balance Sheet, Income Statement, Cash Flow Statement
"""

import pandas as pd
from typing import Dict, List
from datetime import datetime
import logging

from services.cost_basis_tracker import CostBasisTracker

logger = logging.getLogger(__name__)


class FinancialStatementsGenerator:
    """Generate period-based financial statements"""
    
    def __init__(self, transactions_df: pd.DataFrame, cost_basis_tracker: CostBasisTracker):
        self.transactions = transactions_df
        self.tracker = cost_basis_tracker
        
        logger.info("FinancialStatementsGenerator initialized")
    
    def generate_balance_sheet(self, as_of_date: str) -> Dict:
        """Generate balance sheet as of specific date"""
        
        logger.info(f"Generating balance sheet as of {as_of_date}")
        
        as_of_timestamp = int(pd.to_datetime(as_of_date).timestamp())
        
        # Get balances from tracker lots
        assets = []
        total_assets_usd = 0
        
        for token, lots in self.tracker.lots.items():
            if not lots:
                continue
            
            # Filter lots acquired before as_of_date
            relevant_lots = [l for l in lots if l['timestamp'] <= as_of_timestamp]
            if not relevant_lots:
                continue
            
            total_quantity = sum(lot['quantity'] for lot in relevant_lots)
            total_cost = sum(lot['total_cost'] for lot in relevant_lots)
            
            if abs(total_quantity) < 1e-10:
                continue
            
            # Get price at this date
            token_txs = self.transactions[
                (self.transactions['token_contract'] == token) &
                (self.transactions['timestamp'] <= as_of_timestamp)
            ]
            
            price_usd = 0
            token_symbol = 'UNKNOWN'
            
            if len(token_txs) > 0:
                price_series = token_txs['price_usd'].dropna()
                if len(price_series) > 0:
                    price_usd = float(price_series.iloc[-1])
                token_symbol = token_txs.iloc[0]['token_symbol']
            
            value_usd = total_quantity * price_usd
            
            assets.append({
                'asset': token_symbol,
                'contract': token,
                'quantity': float(total_quantity),
                'cost_basis_usd': float(total_cost),
                'price_usd': float(price_usd),
                'value_usd': float(value_usd),
                'unrealized_gain_loss': float(value_usd - total_cost)
            })
            
            total_assets_usd += value_usd
        
        # Sort by value
        assets.sort(key=lambda x: abs(x['value_usd']), reverse=True)
        
        return {
            'as_of_date': as_of_date,
            'assets': assets,
            'total_assets': float(total_assets_usd),
            'liabilities': 0.0,
            'equity': float(total_assets_usd)
        }
    
    def generate_income_statement(self, start_date: str, end_date: str) -> Dict:
        """Generate income statement for period"""
        
        logger.info(f"Generating income statement {start_date} to {end_date}")
        
        start_ts = int(pd.to_datetime(start_date).timestamp())
        end_ts = int(pd.to_datetime(end_date).timestamp())
        
        period_txs = self.transactions[
            (self.transactions['timestamp'] >= start_ts) &
            (self.transactions['timestamp'] <= end_ts)
        ]
        
        # REVENUE
        income_txs = period_txs[period_txs['is_income'] == True]
        
        if not income_txs.empty:
            income_by_type = income_txs.groupby('income_type')['value_usd'].sum().to_dict()
            total_income = float(income_txs['value_usd'].sum())
        else:
            income_by_type = {}
            total_income = 0.0
        
        # REALIZED GAINS
        realized_gains_df = self.tracker.get_realized_gains_for_period(start_date, end_date)
        if not realized_gains_df.empty:
            realized_gains_total = float(realized_gains_df['realized_gain_loss'].sum())
        else:
            realized_gains_total = 0.0
        
        # EXPENSES
        expense_txs = period_txs[period_txs['is_expense'] == True]
        
        if not expense_txs.empty:
            expense_by_type = expense_txs.groupby('expense_type')['value_usd'].sum().to_dict()
            total_expenses = float(expense_txs['value_usd'].sum())
        else:
            expense_by_type = {}
            total_expenses = 0.0
        
        total_gas_fees = float(period_txs['gas_fee_usd'].sum())
        
        # NET INCOME
        total_revenue = total_income + realized_gains_total
        total_costs = total_expenses + total_gas_fees
        net_income = total_revenue - total_costs
        
        return {
            'period_start': start_date,
            'period_end': end_date,
            'revenues': {
                'operating_income': {
                    'by_type': {k: float(v) for k, v in income_by_type.items()},
                    'total': total_income
                },
                'realized_gains_losses': realized_gains_total,
                'total_revenue': total_revenue
            },
            'expenses': {
                'operating_expenses': {
                    'by_type': {k: float(v) for k, v in expense_by_type.items()},
                    'total': total_expenses
                },
                'gas_fees': total_gas_fees,
                'total_expenses': total_costs
            },
            'net_income': net_income,
            'transaction_count': len(period_txs)
        }
    
    def generate_cash_flow_statement(self, start_date: str, end_date: str) -> Dict:
        """Generate cash flow statement for period"""
        
        logger.info(f"Generating cash flow statement {start_date} to {end_date}")
        
        start_ts = int(pd.to_datetime(start_date).timestamp())
        end_ts = int(pd.to_datetime(end_date).timestamp())
        
        period_txs = self.transactions[
            (self.transactions['timestamp'] >= start_ts) &
            (self.transactions['timestamp'] <= end_ts)
        ]
        
        # OPERATING ACTIVITIES
        operating_inflows = float(period_txs[period_txs['is_income'] == True]['value_usd'].sum())
        operating_outflows = float(period_txs[period_txs['is_expense'] == True]['value_usd'].sum())
        gas_fees = float(period_txs['gas_fee_usd'].sum())
        net_operating = operating_inflows - operating_outflows - gas_fees
        
        # INVESTING ACTIVITIES
        investing_txs = period_txs[period_txs['transaction_category'].isin(['trade', 'defi_deposit', 'defi_withdrawal'])]
        investing_inflows = float(investing_txs[investing_txs['direction'] == 'inbound']['value_usd'].sum())
        investing_outflows = float(investing_txs[investing_txs['direction'] == 'outbound']['value_usd'].sum())
        net_investing = investing_inflows - investing_outflows
        
        # FINANCING ACTIVITIES
        financing_txs = period_txs[period_txs['transaction_category'] == 'transfer']
        financing_inflows = float(financing_txs[financing_txs['direction'] == 'inbound']['value_usd'].sum())
        financing_outflows = float(financing_txs[financing_txs['direction'] == 'outbound']['value_usd'].sum())
        net_financing = financing_inflows - financing_outflows
        
        # NET CHANGE
        net_change = net_operating + net_investing + net_financing
        
        return {
            'period_start': start_date,
            'period_end': end_date,
            'operating_activities': {
                'inflows': operating_inflows,
                'outflows': operating_outflows,
                'gas_fees': gas_fees,
                'net': net_operating
            },
            'investing_activities': {
                'inflows': investing_inflows,
                'outflows': investing_outflows,
                'net': net_investing
            },
            'financing_activities': {
                'inflows': financing_inflows,
                'outflows': financing_outflows,
                'net': net_financing
            },
            'net_change_in_cash': net_change
        }
    
    def generate_period_summary(
        self,
        start_date: str,
        end_date: str,
        frequency: str = 'monthly'
    ) -> List[Dict]:
        """Generate summary for periods (monthly, weekly, etc.)"""
        
        logger.info(f"Generating {frequency} summary from {start_date} to {end_date}")
        
        # Map frequency to pandas freq
        freq_map = {
            'daily': 'D',
            'weekly': 'W',
            'monthly': 'M',
            'quarterly': 'Q',
            'yearly': 'Y'
        }
        
        pd_freq = freq_map.get(frequency.lower(), 'M')
        periods = pd.date_range(start=start_date, end=end_date, freq=pd_freq)
        
        summaries = []
        
        for period_end in periods:
            period_start = period_end.to_period(pd_freq).start_time
            
            income_stmt = self.generate_income_statement(
                period_start.strftime('%Y-%m-%d'),
                period_end.strftime('%Y-%m-%d')
            )
            
            summaries.append({
                'period': period_end.strftime('%Y-%m' if frequency == 'monthly' else '%Y-%m-%d'),
                'period_start': period_start.strftime('%Y-%m-%d'),
                'period_end': period_end.strftime('%Y-%m-%d'),
                'total_revenue': income_stmt['revenues']['total_revenue'],
                'total_expenses': income_stmt['expenses']['total_expenses'],
                'net_income': income_stmt['net_income'],
                'transaction_count': income_stmt['transaction_count']
            })
        
        return summaries