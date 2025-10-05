"""
FastAPI Application - Blockchain Wallet Analyzer
Complete REST API for wallet analysis, financial statements, and tax reporting
"""

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

from fastapi import FastAPI, HTTPException, Query, Path, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from typing import Optional, List
import logging
import os
from datetime import datetime

# Import models
from models.responses import (
    WalletAnalysisResponse,
    BalanceSheetResponse,
    FinancialStatementsResponse,
    TaxReportResponse,
    TransactionSummaryResponse,
    Transaction
)

# Import services
from services.cache_manager import CacheManager
from services.wallet_analyzer import WalletAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
cache_manager: Optional[CacheManager] = None
wallet_analyzer: Optional[WalletAnalyzer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for startup and shutdown"""
    # Startup
    global cache_manager, wallet_analyzer
    
    logger.info("Starting Blockchain Wallet Analyzer API...")
    
    # Initialize cache manager
    cache_dir = os.getenv('CACHE_DIR', 'wallet_cache')
    cache_manager = CacheManager(cache_dir=cache_dir)
    
    # Initialize wallet analyzer
    wallet_analyzer = WalletAnalyzer(cache_manager)
    
    logger.info("API initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down API...")


# Initialize FastAPI app
app = FastAPI(
    title="Blockchain Wallet Analyzer API",
    description="Professional-grade blockchain wallet analysis with financial statements and tax reporting",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency injection
async def get_wallet_analyzer() -> WalletAnalyzer:
    """Dependency for wallet analyzer service"""
    if wallet_analyzer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not initialized"
        )
    return wallet_analyzer


async def get_cache_manager() -> CacheManager:
    """Dependency for cache manager service"""
    if cache_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cache service not initialized"
        )
    return cache_manager


# ============================================================================
# HEALTH & INFO ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "service": "Blockchain Wallet Analyzer API",
        "version": "1.0.0",
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "analyze": "/api/v1/wallet/analyze",
            "summary": "/api/v1/wallet/{address}/summary",
            "transactions": "/api/v1/wallet/{address}/transactions",
            "balance_sheet": "/api/v1/wallet/{address}/balance-sheet",
            "financial_statements": "/api/v1/wallet/{address}/financial-statements",
            "tax_report": "/api/v1/wallet/{address}/tax-report"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "cache_manager": cache_manager is not None,
            "wallet_analyzer": wallet_analyzer is not None,
            "api_keys": {
                "etherscan": bool(os.getenv('ETHERSCAN_API_KEY')),
                "moralis": bool(os.getenv('MORALIS_API_KEY')),
            }
        }
    }


# ============================================================================
# WALLET ANALYSIS ENDPOINTS
# ============================================================================

@app.post(
    "/api/v1/wallet/analyze",
    response_model=WalletAnalysisResponse,
    tags=["Wallet Analysis"],
    summary="Analyze wallet transactions"
)
async def analyze_wallet(
    wallet_address: str = Query(..., description="Ethereum wallet address", min_length=42, max_length=42),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    force_refresh: bool = Query(False, description="Force refresh from blockchain"),
    analyzer: WalletAnalyzer = Depends(get_wallet_analyzer)
):
    """
    Perform comprehensive wallet analysis including:
    - Transaction fetching from multiple sources (Etherscan, Moralis)
    - Token metadata enrichment
    - Historical price data
    - Transaction classification
    - Cost basis calculation
    - Realized/unrealized gains
    """
    try:
        logger.info(f"Analyzing wallet: {wallet_address}")
        
        result = await analyzer.analyze_wallet(
            wallet_address=wallet_address,
            start_date=start_date,
            end_date=end_date,
            force_refresh=force_refresh
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Analysis failed for {wallet_address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {str(e)}"
        )


@app.get(
    "/api/v1/wallet/{address}/summary",
    response_model=TransactionSummaryResponse,
    tags=["Wallet Analysis"],
    summary="Get wallet summary"
)
async def get_wallet_summary(
    address: str = Path(..., description="Ethereum wallet address"),
    analyzer: WalletAnalyzer = Depends(get_wallet_analyzer)
):
    """
    Get quick wallet summary from cache.
    If no analysis exists, prompts to run analysis first.
    """
    try:
        result = await analyzer.get_wallet_summary(address)
        return result
        
    except Exception as e:
        logger.error(f"Failed to get summary for {address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve summary: {str(e)}"
        )


@app.get(
    "/api/v1/wallet/{address}/transactions",
    tags=["Wallet Analysis"],
    summary="Get wallet transactions"
)
async def get_transactions(
    address: str = Path(..., description="Ethereum wallet address"),
    limit: int = Query(100, ge=1, le=1000, description="Number of transactions to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    transaction_type: Optional[str] = Query(None, description="Filter by transaction type"),
    analyzer: WalletAnalyzer = Depends(get_wallet_analyzer)
):
    """
    Get paginated transaction list with optional filtering.
    Requires prior wallet analysis.
    """
    try:
        result = await analyzer.get_transactions(
            wallet_address=address,
            limit=limit,
            offset=offset,
            transaction_type=transaction_type
        )
        
        return {
            "wallet_address": address,
            "transactions": result["data"],
            "total": result["total"],
            "limit": limit,
            "offset": offset
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to get transactions for {address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve transactions: {str(e)}"
        )


# ============================================================================
# FINANCIAL STATEMENTS ENDPOINTS
# ============================================================================

@app.get(
    "/api/v1/wallet/{address}/balance-sheet",
    response_model=BalanceSheetResponse,
    tags=["Financial Statements"],
    summary="Get balance sheet"
)
async def get_balance_sheet(
    address: str = Path(..., description="Ethereum wallet address"),
    as_of_date: Optional[str] = Query(None, description="As of date (YYYY-MM-DD), defaults to today"),
    analyzer: WalletAnalyzer = Depends(get_wallet_analyzer)
):
    """
    Generate balance sheet showing assets, liabilities, and equity as of a specific date.
    Includes current holdings with cost basis and unrealized gains.
    """
    try:
        if not as_of_date:
            as_of_date = datetime.now().strftime('%Y-%m-%d')
        
        result = await analyzer.get_balance_sheet(
            wallet_address=address,
            as_of_date=as_of_date
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to generate balance sheet for {address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Balance sheet generation failed: {str(e)}"
        )


@app.get(
    "/api/v1/wallet/{address}/financial-statements",
    response_model=FinancialStatementsResponse,
    tags=["Financial Statements"],
    summary="Get complete financial statements"
)
async def get_financial_statements(
    address: str = Path(..., description="Ethereum wallet address"),
    start_date: Optional[str] = Query(None, description="Period start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end date (YYYY-MM-DD)"),
    period: str = Query("monthly", description="Period frequency: daily, weekly, monthly, quarterly, yearly"),
    analyzer: WalletAnalyzer = Depends(get_wallet_analyzer)
):
    """
    Generate complete financial statements including:
    - Balance Sheet
    - Income Statement
    - Cash Flow Statement
    - Period summaries
    """
    try:
        result = await analyzer.generate_financial_statements(
            wallet_address=address,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to generate statements for {address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Financial statements generation failed: {str(e)}"
        )


# ============================================================================
# TAX REPORTING ENDPOINTS
# ============================================================================

@app.get(
    "/api/v1/wallet/{address}/tax-report",
    response_model=TaxReportResponse,
    tags=["Tax Reporting"],
    summary="Generate tax report"
)
async def get_tax_report(
    address: str = Path(..., description="Ethereum wallet address"),
    tax_year: int = Query(..., ge=2015, le=2025, description="Tax year"),
    analyzer: WalletAnalyzer = Depends(get_wallet_analyzer)
):
    """
    Generate comprehensive tax report including:
    - Capital gains summary (short-term and long-term)
    - Income summary by type
    - Form 8949 entries with cost basis details
    - Transaction count
    """
    try:
        result = await analyzer.generate_tax_report(
            wallet_address=address,
            tax_year=tax_year
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to generate tax report for {address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Tax report generation failed: {str(e)}"
        )


# ============================================================================
# CACHE MANAGEMENT ENDPOINTS
# ============================================================================

@app.get(
    "/api/v1/cache/stats",
    tags=["Cache Management"],
    summary="Get cache statistics"
)
async def get_cache_stats(
    cache: CacheManager = Depends(get_cache_manager)
):
    """
    Get comprehensive cache statistics including:
    - Total cached items
    - Cache size by category
    - Hit/miss rates
    - Performance metrics
    """
    try:
        stats = cache.get_cache_statistics()
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get cache stats: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve cache stats: {str(e)}"
        )


@app.delete(
    "/api/v1/cache/{address}",
    tags=["Cache Management"],
    summary="Clear wallet cache"
)
async def clear_wallet_cache(
    address: str = Path(..., description="Ethereum wallet address"),
    cache: CacheManager = Depends(get_cache_manager)
):
    """
    Clear all cached data for a specific wallet.
    Next analysis will fetch fresh data from blockchain.
    """
    try:
        cleared_count = cache.clear_wallet_cache(address)
        
        return {
            "wallet_address": address,
            "cleared_items": cleared_count,
            "message": f"Cleared {cleared_count} cached items for wallet {address}",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to clear cache for {address}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cache clearing failed: {str(e)}"
        )


@app.delete(
    "/api/v1/cache/category/{category}",
    tags=["Cache Management"],
    summary="Clear cache category"
)
async def clear_cache_category(
    category: str = Path(..., description="Cache category: transactions, metadata, prices, analysis, statements"),
    cache: CacheManager = Depends(get_cache_manager)
):
    """
    Clear all cached data in a specific category.
    """
    try:
        valid_categories = ["transactions", "metadata", "prices", "analysis", "statements"]
        
        if category not in valid_categories:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid category. Must be one of: {', '.join(valid_categories)}"
            )
        
        cleared_count = cache.clear_category(category)
        
        return {
            "category": category,
            "cleared_items": cleared_count,
            "message": f"Cleared {cleared_count} items from {category} cache",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to clear {category} cache: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cache clearing failed: {str(e)}"
        )


@app.delete(
    "/api/v1/cache/all",
    tags=["Cache Management"],
    summary="Clear all cache"
)
async def clear_all_cache(
    cache: CacheManager = Depends(get_cache_manager)
):
    """
    Clear entire cache. Use with caution!
    """
    try:
        cleared_count = cache.clear_all_cache()
        
        return {
            "cleared_items": cleared_count,
            "message": f"Cleared all cache ({cleared_count} items)",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to clear all cache: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cache clearing failed: {str(e)}"
        )


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "status_code": 500,
            "timestamp": datetime.now().isoformat()
        }
    )


# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    reload = os.getenv("API_RELOAD", "true").lower() == "true"
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )