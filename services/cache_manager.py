"""
Smart Cache Manager with Joblib
Implements content-based caching with hash comparison
Only updates when data actually changes
"""

import joblib
import hashlib
import os
from pathlib import Path
from typing import Any, Optional, Dict
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)


class CacheManager:
    """Smart caching system using Joblib with content comparison"""
    
    def __init__(self, cache_dir: str = "wallet_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.cache_dir / "transactions").mkdir(exist_ok=True)
        (self.cache_dir / "metadata").mkdir(exist_ok=True)
        (self.cache_dir / "prices").mkdir(exist_ok=True)
        (self.cache_dir / "analysis").mkdir(exist_ok=True)
        (self.cache_dir / "statements").mkdir(exist_ok=True)
        
        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "updates": 0,
            "no_changes": 0
        }
        
        logger.info(f"CacheManager initialized at {self.cache_dir}")
    
    def _compute_hash(self, data: Any) -> str:
        """Compute SHA256 hash of data"""
        try:
            # Convert data to JSON string for consistent hashing
            data_str = json.dumps(data, sort_keys=True, default=str)
            return hashlib.sha256(data_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Hash computation failed: {e}")
            return ""
    
    def _get_cache_path(self, category: str, key: str) -> Path:
        """Get cache file path"""
        safe_key = key.lower().replace('/', '_').replace('\\', '_')
        return self.cache_dir / category / f"{safe_key}.joblib"
    
    def _get_metadata_path(self, category: str, key: str) -> Path:
        """Get metadata file path"""
        safe_key = key.lower().replace('/', '_').replace('\\', '_')
        return self.cache_dir / category / f"{safe_key}_meta.json"
    
    def get(self, category: str, key: str, max_age_hours: Optional[int] = None) -> Optional[Any]:
        """
        Get cached data if exists and not expired
        
        Args:
            category: Cache category (transactions, metadata, prices, etc.)
            key: Unique identifier
            max_age_hours: Maximum age in hours (None = never expires)
        
        Returns:
            Cached data or None
        """
        cache_path = self._get_cache_path(category, key)
        meta_path = self._get_metadata_path(category, key)
        
        if not cache_path.exists():
            self.stats["misses"] += 1
            return None
        
        # Check age if max_age specified
        if max_age_hours:
            try:
                with open(meta_path, 'r') as f:
                    metadata = json.load(f)
                    cached_time = datetime.fromisoformat(metadata['cached_at'])
                    age = datetime.now() - cached_time
                    
                    if age > timedelta(hours=max_age_hours):
                        logger.info(f"Cache expired for {category}/{key}")
                        self.stats["misses"] += 1
                        return None
            except Exception as e:
                logger.warning(f"Could not read metadata for {category}/{key}: {e}")
        
        try:
            data = joblib.load(cache_path)
            self.stats["hits"] += 1
            logger.debug(f"Cache hit: {category}/{key}")
            return data
        except Exception as e:
            logger.error(f"Failed to load cache {category}/{key}: {e}")
            self.stats["misses"] += 1
            return None
    
    def set(self, category: str, key: str, data: Any, force: bool = False) -> bool:
        """
        Set cached data with smart update (only if changed)
        
        Args:
            category: Cache category
            key: Unique identifier
            data: Data to cache
            force: Force update even if unchanged
        
        Returns:
            True if updated, False if no change
        """
        cache_path = self._get_cache_path(category, key)
        meta_path = self._get_metadata_path(category, key)
        
        # Compute new hash
        new_hash = self._compute_hash(data)
        
        # Check if data changed (unless force)
        if not force and cache_path.exists():
            try:
                with open(meta_path, 'r') as f:
                    old_metadata = json.load(f)
                    old_hash = old_metadata.get('hash', '')
                    
                    if old_hash == new_hash:
                        logger.info(f"No changes detected for {category}/{key}, skipping update")
                        self.stats["no_changes"] += 1
                        return False
            except Exception as e:
                logger.warning(f"Could not compare hashes for {category}/{key}: {e}")
        
        # Save data
        try:
            joblib.dump(data, cache_path, compress=3)
            
            # Save metadata
            metadata = {
                'hash': new_hash,
                'cached_at': datetime.now().isoformat(),
                'category': category,
                'key': key,
                'size_bytes': cache_path.stat().st_size
            }
            
            with open(meta_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.stats["updates"] += 1
            logger.info(f"Cache updated: {category}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save cache {category}/{key}: {e}")
            return False
    
    def delete(self, category: str, key: str) -> bool:
        """Delete cached item"""
        cache_path = self._get_cache_path(category, key)
        meta_path = self._get_metadata_path(category, key)
        
        deleted = False
        if cache_path.exists():
            cache_path.unlink()
            deleted = True
        if meta_path.exists():
            meta_path.unlink()
            deleted = True
        
        if deleted:
            logger.info(f"Cache deleted: {category}/{key}")
        return deleted
    
    def clear_category(self, category: str) -> int:
        """Clear all cache in category"""
        category_path = self.cache_dir / category
        count = 0
        
        if category_path.exists():
            for file in category_path.glob("*"):
                file.unlink()
                count += 1
        
        logger.info(f"Cleared {count} items from {category}")
        return count
    
    def clear_all_cache(self) -> int:
        """Clear entire cache"""
        total_cleared = 0
        for category in ["transactions", "metadata", "prices", "analysis", "statements"]:
            total_cleared += self.clear_category(category)
        
        logger.info(f"Cleared total {total_cleared} cache items")
        return total_cleared
    
    # Specialized cache methods for wallet data
    
    def get_transactions(self, wallet_address: str, tx_type: str = "all") -> Optional[Any]:
        """Get cached transactions"""
        key = f"{wallet_address}_{tx_type}"
        return self.get("transactions", key, max_age_hours=24)
    
    def set_transactions(self, wallet_address: str, tx_type: str, data: Any) -> bool:
        """Cache transactions"""
        key = f"{wallet_address}_{tx_type}"
        return self.set("transactions", key, data)
    
    def get_token_metadata(self, contract_address: str) -> Optional[Dict]:
        """Get cached token metadata"""
        return self.get("metadata", contract_address)
    
    def set_token_metadata(self, contract_address: str, metadata: Dict) -> bool:
        """Cache token metadata"""
        return self.set("metadata", contract_address, metadata)
    
    def get_historical_price(self, contract_address: str, timestamp: int) -> Optional[float]:
        """Get cached historical price"""
        key = f"{contract_address}_{timestamp}"
        result = self.get("prices", key)
        return result
    
    def set_historical_price(self, contract_address: str, timestamp: int, price_data: Dict) -> bool:
        """Cache historical price"""
        key = f"{contract_address}_{timestamp}"
        return self.set("prices", key, price_data)
    
    def get_wallet_analysis(self, wallet_address: str) -> Optional[Dict]:
        """Get cached wallet analysis"""
        return self.get("analysis", wallet_address, max_age_hours=12)
    
    def cache_wallet_analysis(self, wallet_address: str, analysis: Dict) -> bool:
        """Cache wallet analysis"""
        return self.set("analysis", wallet_address, analysis)
    
    def get_financial_statements(self, wallet_address: str) -> Optional[Dict]:
        """Get cached financial statements"""
        return self.get("statements", wallet_address, max_age_hours=12)
    
    def cache_financial_statements(self, wallet_address: str, statements: Dict) -> bool:
        """Cache financial statements"""
        return self.set("statements", wallet_address, statements)
    
    def clear_wallet_cache(self, wallet_address: str) -> int:
        """Clear all cache for specific wallet"""
        cleared = 0
        wallet_lower = wallet_address.lower()
        
        for category in ["transactions", "analysis", "statements"]:
            category_path = self.cache_dir / category
            if category_path.exists():
                for file in category_path.glob(f"{wallet_lower}*"):
                    file.unlink()
                    cleared += 1
        
        logger.info(f"Cleared {cleared} cache items for wallet {wallet_address}")
        return cleared
    
    def get_cache_statistics(self) -> Dict:
        """Get cache statistics"""
        total_items = 0
        total_size = 0
        category_stats = {}
        
        for category in ["transactions", "metadata", "prices", "analysis", "statements"]:
            category_path = self.cache_dir / category
            if category_path.exists():
                items = list(category_path.glob("*.joblib"))
                count = len(items)
                size = sum(f.stat().st_size for f in items)
                
                category_stats[category] = {
                    "count": count,
                    "size_mb": round(size / (1024 * 1024), 2)
                }
                
                total_items += count
                total_size += size
        
        hit_rate = (self.stats["hits"] / (self.stats["hits"] + self.stats["misses"]) * 100 
                   if (self.stats["hits"] + self.stats["misses"]) > 0 else 0)
        
        return {
            "total_cached_items": total_items,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "cache_directory": str(self.cache_dir),
            "categories": category_stats,
            "performance": {
                "cache_hits": self.stats["hits"],
                "cache_misses": self.stats["misses"],
                "hit_rate": round(hit_rate, 2),
                "updates": self.stats["updates"],
                "skipped_updates": self.stats["no_changes"]
            }
        }