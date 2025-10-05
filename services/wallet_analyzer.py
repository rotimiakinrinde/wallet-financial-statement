"""
wallet_analyzer.py
Wallet Analyzer Service
Orchestrates all wallet analysis operations with smart caching
"""

import pandas as pd
from typing import Dict, Optional, List, Any
from datetime import datetime
import logging

from services.cache_manager import CacheManager
from services.blockchain_clients import BlockchainClientManager
from services.transaction_processor import TransactionProcessor
from services.financial_statements import FinancialStatementsGenerator
from services.cost_basis_tracker import CostBasisTracker

logger = logging.getLogger(__name__)


class WalletAnalyzer:
    """Main service for wallet analysis"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache = cache_manager
        self.blockchain_clients = BlockchainClientManager()
        self.transaction_processor = TransactionProcessor(cache_manager)
        self.cost_basis_tracker = CostBasisTracker()
        
        logger.info("WalletAnalyzer initialized")
    
    async def analyze_wallet(
        self,
        wallet_address: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> Dict:
        """
        Complete wallet analysis
        
        Returns comprehensive analysis including:
        - All transactions
        - Token balances
        - Classification
        - Cost basis
        - Gains/losses
        """
        logger.info(f"Starting analysis for {wallet_address}")
        
        # Step 1: Fetch and cache raw transactions
        transactions_df = await self._fetch_all_transactions(wallet_address, force_refresh)
        
        if transactions_df.empty:
            return {
                "wallet_address": wallet_address,
                "status": "no_transactions",
                "message": "No transactions found for this wallet"
            }
        
        # Step 2: Enhance with metadata and pricing
        enhanced_df = await self.transaction_processor.enhance_transactions(
            transactions_df, wallet_address
        )
        
        # Step 3: Classify transactions
        classified_df = self.transaction_processor.classify_transactions(enhanced_df)
        
        # Step 4: Calculate cost basis
        cost_basis_df, tracker = self.cost_basis_tracker.calculate_cost_basis(
            classified_df, wallet_address
        )
        
        # Step 5: Generate summary statistics
        summary = self._generate_summary(cost_basis_df, tracker, wallet_address)
        
        # Cache the complete analysis
        analysis_result = {
            "wallet_address": wallet_address,
            "analysis_date": datetime.now().isoformat(),
            "summary": summary,
            "transactions_count": len(cost_basis_df),
            "date_range": {
                "start": cost_basis_df['date'].min() if not cost_basis_df.empty else None,
                "end": cost_basis_df['date'].max() if not cost_basis_df.empty else None
            },
            "data_sources": ["etherscan", "moralis"],
            "cached": True
        }
        
        # Store processed dataframes in cache as joblib
        self.cache.set("analysis", f"{wallet_address}_transactions", cost_basis_df)
        self.cache.set("analysis", f"{wallet_address}_tracker", tracker)
        
        logger.info(f"Analysis complete for {wallet_address}")
        return analysis_result
    
    async def _fetch_all_transactions(
        self, 
        wallet_address: str, 
        force_refresh: bool
    ) -> pd.DataFrame:
        """Fetch all transaction types with smart caching"""
        
        # Check cache first
        if not force_refresh:
            cached_txs = self.cache.get_transactions(wallet_address, "all")
            if cached_txs is not None:
                logger.info(f"Using cached transactions for {wallet_address}")
                return cached_txs
        
        logger.info(f"Fetching fresh transactions for {wallet_address}")
        
        all_txs = []
        
        # Fetch from Etherscan
        etherscan_txs = await self.blockchain_clients.fetch_etherscan_transactions(
            wallet_address
        )
        if not etherscan_txs.empty:
            all_txs.append(etherscan_txs)
            logger.info(f"Fetched {len(etherscan_txs)} Etherscan transactions")
        
        # Fetch from Moralis
        moralis_txs = await self.blockchain_clients.fetch_moralis_transactions(
            wallet_address
        )
        if not moralis_txs.empty:
            all_txs.append(moralis_txs)
            logger.info(f"Fetched {len(moralis_txs)} Moralis transactions")
        
        # Merge and deduplicate
        if all_txs:
            merged_df = pd.concat(all_txs, ignore_index=True)
            merged_df = merged_df.drop_duplicates(subset=['hash'], keep='first')
            merged_df = merged_df.sort_values('timestamp', ascending=False)
            
            # Cache with smart update
            updated = self.cache.set_transactions(wallet_address, "all", merged_df)
            if updated:
                logger.info(f"Cached {len(merged_df)} transactions for {wallet_address}")
            else:
                logger.info(f"Transactions unchanged for {wallet_address}")
            
            return merged_df
        
        return pd.DataFrame()
    
    def _generate_summary(
        self, 
        transactions_df: pd.DataFrame, 
        tracker: CostBasisTracker,
        wallet_address: str
    ) -> Dict:
        """Generate summary statistics"""
        
        if transactions_df.empty:
            return {}
        
        # Transaction breakdown
        total_txs = len(transactions_df)
        inbound = len(transactions_df[transactions_df['direction'] == 'inbound'])
        outbound = len(transactions_df[transactions_df['direction'] == 'outbound'])
        
        # Token statistics
        unique_tokens = transactions_df['token_symbol'].nunique()
        top_tokens = transactions_df['token_symbol'].value_counts().head(10).to_dict()
        
        # Financial metrics
        total_value_usd = float(transactions_df['value_usd'].sum())
        total_gas_fees = float(transactions_df['gas_fee_usd'].sum())
        
        # Income/Expense breakdown
        income_txs = transactions_df[transactions_df['is_income'] == True]
        expense_txs = transactions_df[transactions_df['is_expense'] == True]
        
        total_income = float(income_txs['value_usd'].sum()) if not income_txs.empty else 0
        total_expenses = float(expense_txs['value_usd'].sum()) if not expense_txs.empty else 0
        
        # Gains/Losses
        realized_gains = tracker.get_total_realized_gains()
        unrealized_gains = tracker.get_total_unrealized_gains()
        
        return {
            "transaction_breakdown": {
                "total": total_txs,
                "inbound": inbound,
                "outbound": outbound
            },
            "token_statistics": {
                "unique_tokens": unique_tokens,
                "top_tokens": top_tokens
            },
            "financial_metrics": {
                "total_value_usd": total_value_usd,
                "total_gas_fees_usd": total_gas_fees,
                "total_income_usd": total_income,
                "total_expenses_usd": total_expenses,
                "net_income_usd": total_income - total_expenses
            },
            "gains_losses": {
                "realized_gains_usd": realized_gains,
                "unrealized_gains_usd": unrealized_gains,
                "total_gains_usd": realized_gains + unrealized_gains
            }
        }
    
    async def generate_financial_statements(
        self,
        wallet_address: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "monthly"
    ) -> Dict:
        """Generate complete financial statements"""
        
        logger.info(f"Generating financial statements for {wallet_address}")
        
        # Get processed transactions from cache
        transactions_df = self.cache.get("analysis", f"{wallet_address}_transactions")
        tracker = self.cache.get("analysis", f"{wallet_address}_tracker")
        
        if transactions_df is None or tracker is None:
            raise ValueError("No analysis data found. Run analyze_wallet first.")
        
        # Use date range from data if not specified
        if not start_date:
            start_date = transactions_df['date'].min()
        if not end_date:
            end_date = transactions_df['date'].max()
        
        # Generate statements
        generator = FinancialStatementsGenerator(transactions_df, tracker)
        
        balance_sheet = generator.generate_balance_sheet(end_date)
        income_statement = generator.generate_income_statement(start_date, end_date)
        cash_flow_statement = generator.generate_cash_flow_statement(start_date, end_date)
        period_summary = generator.generate_period_summary(start_date, end_date, period)
        
        statements = {
            "wallet_address": wallet_address,
            "generated_at": datetime.now().isoformat(),
            "period": {
                "start": start_date,
                "end": end_date,
                "frequency": period
            },
            "balance_sheet": balance_sheet,
            "income_statement": income_statement,
            "cash_flow_statement": cash_flow_statement,
            "period_summary": period_summary
        }
        
        logger.info(f"Financial statements generated for {wallet_address}")
        return statements
    
    async def get_balance_sheet(
        self,
        wallet_address: str,
        as_of_date: str
    ) -> Dict:
        """Get balance sheet as of specific date"""
        
        transactions_df = self.cache.get("analysis", f"{wallet_address}_transactions")
        tracker = self.cache.get("analysis", f"{wallet_address}_tracker")
        
        if transactions_df is None or tracker is None:
            raise ValueError("No analysis data found. Run analyze_wallet first.")
        
        generator = FinancialStatementsGenerator(transactions_df, tracker)
        balance_sheet = generator.generate_balance_sheet(as_of_date)
        
        return {
            "wallet_address": wallet_address,
            "as_of_date": as_of_date,
            "balance_sheet": balance_sheet
        }
    
    async def generate_tax_report(
        self,
        wallet_address: str,
        tax_year: int
    ) -> Dict:
        """Generate tax report for specific year"""
        
        logger.info(f"Generating tax report for {wallet_address}, year {tax_year}")
        
        transactions_df = self.cache.get("analysis", f"{wallet_address}_transactions")
        tracker = self.cache.get("analysis", f"{wallet_address}_tracker")
        
        if transactions_df is None or tracker is None:
            raise ValueError("No analysis data found. Run analyze_wallet first.")
        
        # Filter transactions for tax year
        start_date = f"{tax_year}-01-01"
        end_date = f"{tax_year}-12-31"
        
        year_txs = transactions_df[
            (transactions_df['date'] >= start_date) &
            (transactions_df['date'] <= end_date)
        ]
        
        # Get realized gains for the year
        realized_gains = tracker.get_realized_gains_for_period(start_date, end_date)
        
        # Calculate tax summary
        short_term_gains = 0
        short_term_losses = 0
        long_term_gains = 0
        long_term_losses = 0
        
        form_8949_entries = []
        
        for _, disposal in realized_gains.iterrows():
            lots_used = disposal.get('lots_used', [])
            
            for lot in lots_used:
                holding_days = lot.get('holding_period_days', 0)
                gain_loss = lot.get('proceeds', 0) - lot['cost_basis']
                
                if holding_days <= 365:
                    if gain_loss > 0:
                        short_term_gains += gain_loss
                    else:
                        short_term_losses += abs(gain_loss)
                    term = "Short-term"
                else:
                    if gain_loss > 0:
                        long_term_gains += gain_loss
                    else:
                        long_term_losses += abs(gain_loss)
                    term = "Long-term"
                
                # Get token symbol
                token_symbol = "UNKNOWN"
                token_txs = transactions_df[transactions_df['token_contract'] == disposal['token']]
                if len(token_txs) > 0:
                    token_symbol = token_txs.iloc[0]['token_symbol']
                
                form_8949_entries.append({
                    "description": f"{lot['quantity']:.6f} {token_symbol}",
                    "date_acquired": lot['acquisition_date'],
                    "date_sold": disposal['disposal_date'],
                    "proceeds": float(lot.get('proceeds', 0)),
                    "cost_basis": float(lot['cost_basis']),
                    "gain_loss": float(gain_loss),
                    "term": term,
                    "tx_hash": disposal['tx_hash']
                })
        
        # Income summary
        income_txs = year_txs[year_txs['is_income'] == True]
        income_by_type = {}
        if not income_txs.empty:
            income_by_type = {k: float(v) for k, v in income_txs.groupby('income_type')['value_usd'].sum().to_dict().items()}
        
        tax_report = {
            "wallet_address": wallet_address,
            "tax_year": tax_year,
            "generated_at": datetime.now().isoformat(),
            "capital_gains_summary": {
                "short_term": {
                    "gains": float(short_term_gains),
                    "losses": float(short_term_losses),
                    "net": float(short_term_gains - short_term_losses)
                },
                "long_term": {
                    "gains": float(long_term_gains),
                    "losses": float(long_term_losses),
                    "net": float(long_term_gains - long_term_losses)
                },
                "total_net": float((short_term_gains - short_term_losses) + (long_term_gains - long_term_losses))
            },
            "income_summary": {
                "by_type": income_by_type,
                "total": float(income_txs['value_usd'].sum()) if not income_txs.empty else 0.0
            },
            "form_8949_entries": form_8949_entries,
            "transaction_count": len(year_txs)
        }
        
        logger.info(f"Tax report generated for {wallet_address}")
        return tax_report
    
    async def get_transactions(
        self,
        wallet_address: str,
        limit: int = 100,
        offset: int = 0,
        transaction_type: Optional[str] = None
    ) -> Dict:
        """Get paginated transactions"""
        
        transactions_df = self.cache.get("analysis", f"{wallet_address}_transactions")
        
        if transactions_df is None:
            return {"data": [], "total": 0}
        
        # Filter by type if specified
        if transaction_type:
            filtered_df = transactions_df[transactions_df['transaction_type'] == transaction_type]
        else:
            filtered_df = transactions_df
        
        total = len(filtered_df)
        
        # Paginate
        paginated_df = filtered_df.iloc[offset:offset+limit]
        
        # Convert to records
        transactions = paginated_df.to_dict('records')
        
        # Clean up numpy types for JSON serialization
        for tx in transactions:
            for key, value in tx.items():
                if pd.isna(value):
                    tx[key] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    tx[key] = value.isoformat()
                elif hasattr(value, 'item'):  # numpy types
                    tx[key] = value.item()
        
        return {
            "data": transactions,
            "total": total
        }
    
    async def get_wallet_summary(self, wallet_address: str) -> Dict:
        """Get quick wallet summary"""
        
        # Check if analysis exists
        cached_analysis = self.cache.get_wallet_analysis(wallet_address)
        
        if cached_analysis:
            return {
                "wallet_address": wallet_address,
                "summary": cached_analysis.get('summary', {}),
                "last_updated": cached_analysis.get('analysis_date'),
                "cached": True
            }
        
        # If no cache, return basic info
        return {
            "wallet_address": wallet_address,
            "status": "not_analyzed",
            "message": "No analysis available. Run /api/v1/wallet/analyze first.",
            "cached": False
        }