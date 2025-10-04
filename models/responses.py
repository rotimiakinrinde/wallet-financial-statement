"""
Pydantic Response Models
Defines API response schemas
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime


class TransactionSummary(BaseModel):
    """Transaction breakdown summary"""
    total: int = Field(..., description="Total number of transactions")
    inbound: int = Field(..., description="Inbound transactions")
    outbound: int = Field(..., description="Outbound transactions")


class TokenStatistics(BaseModel):
    """Token statistics"""
    unique_tokens: int = Field(..., description="Number of unique tokens")
    top_tokens: Dict[str, int] = Field(..., description="Top tokens by transaction count")


class FinancialMetrics(BaseModel):
    """Financial metrics summary"""
    total_value_usd: float = Field(..., description="Total transaction value in USD")
    total_gas_fees_usd: float = Field(..., description="Total gas fees in USD")
    total_income_usd: float = Field(..., description="Total income in USD")
    total_expenses_usd: float = Field(..., description="Total expenses in USD")
    net_income_usd: float = Field(..., description="Net income in USD")


class GainsLosses(BaseModel):
    """Gains and losses summary"""
    realized_gains_usd: float = Field(..., description="Realized gains/losses")
    unrealized_gains_usd: float = Field(..., description="Unrealized gains/losses")
    total_gains_usd: float = Field(..., description="Total gains/losses")


class WalletSummary(BaseModel):
    """Wallet summary data"""
    transaction_breakdown: TransactionSummary
    token_statistics: TokenStatistics
    financial_metrics: FinancialMetrics
    gains_losses: GainsLosses


class DateRange(BaseModel):
    """Date range"""
    start: Optional[str] = Field(None, description="Start date")
    end: Optional[str] = Field(None, description="End date")


class WalletAnalysisResponse(BaseModel):
    """Complete wallet analysis response"""
    wallet_address: str = Field(..., description="Wallet address analyzed")
    analysis_date: str = Field(..., description="Timestamp of analysis")
    summary: WalletSummary = Field(..., description="Analysis summary")
    transactions_count: int = Field(..., description="Total transactions")
    date_range: DateRange = Field(..., description="Date range of transactions")
    data_sources: List[str] = Field(..., description="Data sources used")
    cached: bool = Field(..., description="Whether result is from cache")


class Asset(BaseModel):
    """Balance sheet asset"""
    asset: str = Field(..., description="Token symbol")
    contract: str = Field(..., description="Contract address")
    quantity: float = Field(..., description="Token quantity")
    cost_basis_usd: float = Field(..., description="Cost basis in USD")
    price_usd: float = Field(..., description="Current price in USD")
    value_usd: float = Field(..., description="Current value in USD")
    unrealized_gain_loss: float = Field(..., description="Unrealized gain/loss")


class BalanceSheet(BaseModel):
    """Balance sheet data"""
    as_of_date: str = Field(..., description="Balance sheet date")
    assets: List[Asset] = Field(..., description="List of assets")
    total_assets: float = Field(..., description="Total assets value")
    liabilities: float = Field(..., description="Total liabilities")
    equity: float = Field(..., description="Total equity")


class BalanceSheetResponse(BaseModel):
    """Balance sheet API response"""
    wallet_address: str
    as_of_date: str
    balance_sheet: BalanceSheet


class RevenueBreakdown(BaseModel):
    """Revenue breakdown"""
    operating_income: Dict[str, Any]
    realized_gains_losses: float
    total_revenue: float


class ExpenseBreakdown(BaseModel):
    """Expense breakdown"""
    operating_expenses: Dict[str, Any]
    gas_fees: float
    total_expenses: float


class IncomeStatement(BaseModel):
    """Income statement data"""
    period_start: str
    period_end: str
    revenues: RevenueBreakdown
    expenses: ExpenseBreakdown
    net_income: float
    transaction_count: int


class CashFlowActivity(BaseModel):
    """Cash flow activity"""
    inflows: float
    outflows: float
    net: float


class CashFlowStatement(BaseModel):
    """Cash flow statement data"""
    period_start: str
    period_end: str
    operating_activities: CashFlowActivity
    investing_activities: CashFlowActivity
    financing_activities: CashFlowActivity
    net_change_in_cash: float


class PeriodSummary(BaseModel):
    """Period summary"""
    period: str
    period_start: str
    period_end: str
    total_revenue: float
    total_expenses: float
    net_income: float
    transaction_count: int


class FinancialStatements(BaseModel):
    """Complete financial statements"""
    balance_sheet: BalanceSheet
    income_statement: IncomeStatement
    cash_flow_statement: CashFlowStatement
    period_summary: List[PeriodSummary]


class FinancialStatementsResponse(BaseModel):
    """Financial statements API response"""
    wallet_address: str
    generated_at: str
    period: Dict[str, str]
    balance_sheet: BalanceSheet
    income_statement: IncomeStatement
    cash_flow_statement: CashFlowStatement
    period_summary: List[PeriodSummary]


class CapitalGainTerm(BaseModel):
    """Capital gain term breakdown"""
    gains: float
    losses: float
    net: float


class CapitalGainsSummary(BaseModel):
    """Capital gains summary"""
    short_term: CapitalGainTerm
    long_term: CapitalGainTerm
    total_net: float


class Form8949Entry(BaseModel):
    """Form 8949 entry"""
    description: str
    date_acquired: str
    date_sold: str
    proceeds: float
    cost_basis: float
    gain_loss: float
    term: str
    tx_hash: str


class IncomeSummary(BaseModel):
    """Income summary"""
    by_type: Dict[str, float]
    total: float


class TaxReport(BaseModel):
    """Tax report data"""
    capital_gains_summary: CapitalGainsSummary
    income_summary: IncomeSummary
    form_8949_entries: List[Form8949Entry]
    transaction_count: int


class TaxReportResponse(BaseModel):
    """Tax report API response"""
    wallet_address: str
    tax_year: int
    generated_at: str
    capital_gains_summary: CapitalGainsSummary
    income_summary: IncomeSummary
    form_8949_entries: List[Form8949Entry]
    transaction_count: int


class Transaction(BaseModel):
    """Single transaction"""
    hash: str
    timestamp: int
    date: str
    direction: str
    token_symbol: str
    value_normalized: float
    value_usd: float
    transaction_type: str
    gas_fee_usd: float


class TransactionSummaryResponse(BaseModel):
    """Transaction summary response"""
    wallet_address: str
    summary: WalletSummary
    last_updated: Optional[str]
    cached: bool