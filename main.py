"""
FastAPI Wallet Financial Statement System
Production-ready with Joblib caching and smart updates
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
from datetime import datetime
import logging
import os

from services.wallet_analyzer import WalletAnalyzer
from services.cache_manager import CacheManager
from models.responses import (
    WalletAnalysisResponse,
    FinancialStatementsResponse,
    TaxReportResponse,
    BalanceSheetResponse,
    TransactionSummaryResponse
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Blockchain Wallet Financial Statement API",
    description="Generate comprehensive financial statements from blockchain wallet data",
    version="1.0.0"
)

# Configure CORS for Vercel frontend
origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
cache_manager = CacheManager(cache_dir="wallet_cache")
wallet_analyzer = WalletAnalyzer(cache_manager=cache_manager)


# Request Models
class WalletAnalysisRequest(BaseModel):
    wallet_address: str = Field(..., description="Ethereum wallet address")
    force_refresh: bool = Field(False, description="Force data refresh ignoring cache")
    start_date: Optional[str] = Field(None, description="Analysis start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="Analysis end date (YYYY-MM-DD)")


class FinancialStatementsRequest(BaseModel):
    wallet_address: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    period: str = Field("monthly", description="Period: daily, weekly, monthly, yearly")


# Health Check
@app.get("/")
async def root():
    """API health check"""
    return {
        "status": "online",
        "service": "Wallet Financial Statement API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health_check():
    """Detailed health check"""
    cache_stats = cache_manager.get_cache_statistics()
    
    return {
        "status": "healthy",
        "cache_size": cache_stats["total_cached_items"],
        "cache_hit_rate": cache_stats.get("hit_rate", 0),
        "timestamp": datetime.now().isoformat()
    }


# Main Analysis Endpoint
@app.post("/api/v1/wallet/analyze", response_model=WalletAnalysisResponse)
async def analyze_wallet(
    request: WalletAnalysisRequest,
    background_tasks: BackgroundTasks
):
    """
    Comprehensive wallet analysis with all financial data
    
    Returns:
    - Transaction summary
    - Token balances
    - Ledger data
    - Classification statistics
    - Caching metadata
    """
    try:
        logger.info(f"Starting analysis for wallet: {request.wallet_address}")
        
        # Check cache first (unless force refresh)
        if not request.force_refresh:
            cached_analysis = cache_manager.get_wallet_analysis(request.wallet_address)
            if cached_analysis:
                logger.info(f"Returning cached analysis for {request.wallet_address}")
                return cached_analysis
        
        # Perform fresh analysis
        analysis_result = await wallet_analyzer.analyze_wallet(
            wallet_address=request.wallet_address,
            start_date=request.start_date,
            end_date=request.end_date,
            force_refresh=request.force_refresh
        )
        
        # Cache the result
        background_tasks.add_task(
            cache_manager.cache_wallet_analysis,
            request.wallet_address,
            analysis_result
        )
        
        logger.info(f"Analysis complete for {request.wallet_address}")
        return analysis_result
        
    except Exception as e:
        logger.error(f"Analysis failed for {request.wallet_address}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@app.post("/api/v1/wallet/statements", response_model=FinancialStatementsResponse)
async def generate_financial_statements(
    request: FinancialStatementsRequest,
    background_tasks: BackgroundTasks
):
    """
    Generate complete financial statements
    
    Returns:
    - Balance Sheet
    - Income Statement
    - Cash Flow Statement
    - Monthly/Period summaries
    """
    try:
        logger.info(f"Generating statements for {request.wallet_address}")
        
        # Check if analysis exists in cache
        cached_analysis = cache_manager.get_wallet_analysis(request.wallet_address)
        
        if not cached_analysis:
            # Need to run analysis first
            logger.info("No cached analysis found, running analysis first")
            analysis_result = await wallet_analyzer.analyze_wallet(
                wallet_address=request.wallet_address,
                start_date=request.start_date,
                end_date=request.end_date
            )
            background_tasks.add_task(
                cache_manager.cache_wallet_analysis,
                request.wallet_address,
                analysis_result
            )
        
        # Generate statements
        statements = await wallet_analyzer.generate_financial_statements(
            wallet_address=request.wallet_address,
            start_date=request.start_date,
            end_date=request.end_date,
            period=request.period
        )
        
        # Cache statements
        background_tasks.add_task(
            cache_manager.cache_financial_statements,
            request.wallet_address,
            statements
        )
        
        logger.info(f"Statements generated for {request.wallet_address}")
        return statements
        
    except Exception as e:
        logger.error(f"Statement generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Statement generation failed: {str(e)}")


@app.get("/api/v1/wallet/{wallet_address}/balance-sheet", response_model=BalanceSheetResponse)
async def get_balance_sheet(
    wallet_address: str,
    as_of_date: Optional[str] = Query(None, description="Balance sheet date (YYYY-MM-DD)")
):
    """Get balance sheet as of specific date"""
    try:
        balance_sheet = await wallet_analyzer.get_balance_sheet(
            wallet_address=wallet_address,
            as_of_date=as_of_date or datetime.now().strftime('%Y-%m-%d')
        )
        return balance_sheet
        
    except Exception as e:
        logger.error(f"Balance sheet generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/wallet/{wallet_address}/tax-report", response_model=TaxReportResponse)
async def get_tax_report(
    wallet_address: str,
    tax_year: Optional[int] = Query(None, description="Tax year (default: current year)")
):
    """
    Generate tax report (Form 8949 ready)
    
    Returns:
    - Short-term gains/losses
    - Long-term gains/losses
    - Detailed transaction list
    """
    try:
        tax_year = tax_year or datetime.now().year
        
        tax_report = await wallet_analyzer.generate_tax_report(
            wallet_address=wallet_address,
            tax_year=tax_year
        )
        
        return tax_report
        
    except Exception as e:
        logger.error(f"Tax report generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/wallet/{wallet_address}/transactions")
async def get_transactions(
    wallet_address: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    transaction_type: Optional[str] = Query(None, description="Filter by type: normal, erc20, internal")
):
    """Get paginated transaction list"""
    try:
        transactions = await wallet_analyzer.get_transactions(
            wallet_address=wallet_address,
            limit=limit,
            offset=offset,
            transaction_type=transaction_type
        )
        
        return {
            "wallet_address": wallet_address,
            "transactions": transactions["data"],
            "total": transactions["total"],
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Transaction fetch failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/wallet/{wallet_address}/summary", response_model=TransactionSummaryResponse)
async def get_wallet_summary(wallet_address: str):
    """Get quick wallet summary"""
    try:
        summary = await wallet_analyzer.get_wallet_summary(wallet_address)
        return summary
        
    except Exception as e:
        logger.error(f"Summary generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Cache Management Endpoints
@app.delete("/api/v1/cache/{wallet_address}")
async def clear_wallet_cache(wallet_address: str):
    """Clear cache for specific wallet"""
    try:
        cache_manager.clear_wallet_cache(wallet_address)
        return {
            "status": "success",
            "message": f"Cache cleared for {wallet_address}",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/cache")
async def clear_all_cache():
    """Clear entire cache (use with caution)"""
    try:
        cache_manager.clear_all_cache()
        return {
            "status": "success",
            "message": "All cache cleared",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/cache/stats")
async def get_cache_stats():
    """Get cache statistics"""
    try:
        stats = cache_manager.get_cache_statistics()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "timestamp": datetime.now().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)