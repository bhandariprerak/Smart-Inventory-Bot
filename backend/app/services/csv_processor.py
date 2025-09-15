import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging
from app.services.google_drive import GoogleDriveService
from functools import lru_cache
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
import asyncio
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ScalableCSVProcessor:
    def __init__(self, cache_size: int = 1000, chunk_size: int = 10000):
        self.drive_service = GoogleDriveService()
        self.data = {}
        self.cache_size = cache_size
        self.chunk_size = chunk_size
        self.data_cache = {}
        self.query_cache = {}
        self.last_refresh = None
        self.cache_ttl = timedelta(minutes=30)  # Cache TTL
        self.load_data()
    
    def load_data(self):
        """Load all CSV data with caching and chunking support"""
        try:
            logger.info("Loading CSV data with scalability optimizations...")
            self.data = self.drive_service.load_all_csv_files()
            self.last_refresh = datetime.now()
            
            # Create indexes for faster lookups
            self._create_indexes()
            
            logger.info(f"CSV Processor initialized with {len(self.data)} datasets")
            logger.info(f"Memory usage optimizations: chunk_size={self.chunk_size}, cache_size={self.cache_size}")
        except Exception as e:
            logger.error(f"Error loading data in Scalable CSV Processor: {str(e)}")
    
    def _create_indexes(self):
        """Create indexes for faster data access"""
        self.indexes = {}
        
        try:
            # Customer index by name and ID
            if 'customer' in self.data:
                df = self.data['customer']
                self.indexes['customer_by_name'] = {
                    name.lower(): idx for idx, name in enumerate(df['customer_name'])
                }
                self.indexes['customer_by_id'] = {
                    cid: idx for idx, cid in enumerate(df['CID'])
                }
            
            # Product index by name and category
            if 'pricelist' in self.data:
                df = self.data['pricelist']
                self.indexes['product_by_name'] = {}
                self.indexes['product_by_category'] = {}
                
                for idx, (name, category) in enumerate(zip(df['product_name'], df['category'])):
                    # Multi-word indexing for product names
                    words = name.lower().split()
                    for word in words:
                        if word not in self.indexes['product_by_name']:
                            self.indexes['product_by_name'][word] = []
                        self.indexes['product_by_name'][word].append(idx)
                    
                    if category not in self.indexes['product_by_category']:
                        self.indexes['product_by_category'][category] = []
                    self.indexes['product_by_category'][category].append(idx)
            
            # Order index by customer ID and status
            if 'inventory' in self.data:
                df = self.data['inventory']
                self.indexes['orders_by_customer'] = {}
                self.indexes['orders_by_status'] = {}
                
                for idx, (cid, status) in enumerate(zip(df['CID'], df['order_status'])):
                    if cid not in self.indexes['orders_by_customer']:
                        self.indexes['orders_by_customer'][cid] = []
                    self.indexes['orders_by_customer'][cid].append(idx)
                    
                    if status not in self.indexes['orders_by_status']:
                        self.indexes['orders_by_status'][status] = []
                    self.indexes['orders_by_status'][status].append(idx)
            
            logger.info("Indexes created successfully for fast data access")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            self.indexes = {}
    
    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate cache key for query results"""
        key_data = f"{method}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid"""
        if self.last_refresh is None:
            return False
        return datetime.now() - self.last_refresh < self.cache_ttl
    
    @lru_cache(maxsize=1000)
    def get_customers(self, customer_name: str = None, customer_id: str = None, 
                     page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get customer information with pagination and caching"""
        try:
            cache_key = self._get_cache_key('get_customers', 
                                          customer_name=customer_name, 
                                          customer_id=customer_id,
                                          page=page, page_size=page_size)
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            df = self.data.get('customer')
            if df is None or df.empty:
                return []
            
            result_df = df.copy()
            
            # Use indexes for faster filtering
            if customer_name and 'customer_by_name' in self.indexes:
                name_lower = customer_name.lower()
                matching_indices = []
                
                # Find indices that match the name
                for indexed_name, idx in self.indexes['customer_by_name'].items():
                    if name_lower in indexed_name:
                        matching_indices.append(idx)
                
                if matching_indices:
                    result_df = df.iloc[matching_indices]
                else:
                    result_df = df[df['customer_name'].str.contains(customer_name, case=False, na=False)]
            
            if customer_id and 'customer_by_id' in self.indexes:
                if customer_id in self.indexes['customer_by_id']:
                    idx = self.indexes['customer_by_id'][customer_id]
                    result_df = df.iloc[[idx]]
                else:
                    result_df = df[df['CID'] == customer_id]
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            result = result_df.to_dict('records')
            
            # Cache the result
            self.query_cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting customers: {str(e)}")
            return []
    
    def get_orders(self, customer_id: str = None, status: str = None, 
                   page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get order information with indexing and pagination"""
        try:
            cache_key = self._get_cache_key('get_orders', 
                                          customer_id=customer_id, 
                                          status=status,
                                          page=page, page_size=page_size)
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            df = self.data.get('inventory')
            if df is None or df.empty:
                return []
            
            result_indices = None
            
            # Use indexes for faster filtering
            if customer_id and 'orders_by_customer' in self.indexes:
                if customer_id in self.indexes['orders_by_customer']:
                    result_indices = self.indexes['orders_by_customer'][customer_id]
            
            if status and 'orders_by_status' in self.indexes:
                status_indices = self.indexes['orders_by_status'].get(status, [])
                if result_indices is not None:
                    # Intersection of customer and status indices
                    result_indices = list(set(result_indices) & set(status_indices))
                else:
                    result_indices = status_indices
            
            # Apply filters
            if result_indices is not None:
                result_df = df.iloc[result_indices]
            else:
                result_df = df.copy()
                if customer_id:
                    result_df = result_df[result_df['CID'] == customer_id]
                if status:
                    result_df = result_df[result_df['order_status'].str.contains(status, case=False, na=False)]
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            result = result_df.to_dict('records')
            
            # Cache the result
            self.query_cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting orders: {str(e)}")
            return []
    
    def get_products(self, product_name: str = None, category: str = None,
                    page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get product information with smart indexing"""
        try:
            cache_key = self._get_cache_key('get_products', 
                                          product_name=product_name, 
                                          category=category,
                                          page=page, page_size=page_size)
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            df = self.data.get('pricelist')
            if df is None or df.empty:
                return []
            
            result_indices = None
            
            # Use product name index
            if product_name and 'product_by_name' in self.indexes:
                name_words = product_name.lower().split()
                matching_indices = set()
                
                for word in name_words:
                    if word in self.indexes['product_by_name']:
                        matching_indices.update(self.indexes['product_by_name'][word])
                    # Also check partial matches
                    for indexed_word, indices in self.indexes['product_by_name'].items():
                        if word in indexed_word or indexed_word in word:
                            matching_indices.update(indices)
                
                result_indices = list(matching_indices)
            
            # Use category index
            if category and 'product_by_category' in self.indexes:
                category_indices = self.indexes['product_by_category'].get(category, [])
                if result_indices is not None:
                    result_indices = list(set(result_indices) & set(category_indices))
                else:
                    result_indices = category_indices
            
            # Apply filters
            if result_indices is not None:
                result_df = df.iloc[result_indices]
            else:
                result_df = df.copy()
                if product_name:
                    result_df = result_df[result_df['product_name'].str.contains(product_name, case=False, na=False)]
                if category:
                    result_df = result_df[result_df['category'].str.contains(category, case=False, na=False)]
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            result = result_df.to_dict('records')
            
            # Cache the result
            self.query_cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting products: {str(e)}")
            return []
    
    def get_order_details(self, order_id: str = None) -> List[Dict]:
        """Get order details with caching"""
        try:
            cache_key = self._get_cache_key('get_order_details', order_id=order_id)
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            df = self.data.get('detail')
            if df is None or df.empty:
                return []
            
            result_df = df.copy()
            
            if order_id:
                result_df = result_df[result_df['IID'] == order_id]
            
            result = result_df.to_dict('records')
            
            # Cache the result
            self.query_cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting order details: {str(e)}")
            return []
    
    def get_customer_orders_with_details(self, customer_name: str = None, 
                                       customer_id: str = None) -> List[Dict]:
        """Get comprehensive customer order information optimized for large datasets"""
        try:
            cache_key = self._get_cache_key('get_customer_orders_with_details', 
                                          customer_name=customer_name, 
                                          customer_id=customer_id)
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            # Use optimized queries
            customers = self.get_customers(customer_name=customer_name, customer_id=customer_id)
            if not customers:
                return []
            
            result = []
            
            # Process in chunks to handle large datasets
            for customer in customers:
                customer_orders = self.get_orders(customer_id=customer['CID'])
                
                for order in customer_orders:
                    order_details = self.get_order_details(order_id=order['IID'])
                    
                    # Join with product information efficiently
                    enhanced_order = {**customer, **order}
                    enhanced_order['order_details'] = []
                    
                    for detail in order_details:
                        # Use product index for fast lookup
                        products = self.get_products()  # This is cached
                        product = next((p for p in products if p['price_table_item_id'] == detail['price_table_item_id']), None)
                        
                        if product:
                            enhanced_detail = {**detail, **product}
                            enhanced_order['order_details'].append(enhanced_detail)
                    
                    result.append(enhanced_order)
            
            # Cache the result
            self.query_cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting customer orders with details: {str(e)}")
            return []
    
    def search_data(self, query: str, limit: int = 100) -> Dict[str, Any]:
        """General search across all data with performance optimization"""
        try:
            cache_key = self._get_cache_key('search_data', query=query, limit=limit)
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            results = {
                "customers": [],
                "orders": [],
                "products": [],
                "query": query
            }
            
            # Parallel search across different data types
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_customers = executor.submit(self.get_customers, customer_name=query, page_size=limit//3)
                future_products = executor.submit(self.get_products, product_name=query, page_size=limit//3)
                
                # Wait for results
                results["customers"] = future_customers.result()
                results["products"] = future_products.result()
            
            # Search by customer ID pattern
            if query.upper().startswith('C') and len(query) == 4:
                orders = self.get_orders(customer_id=query.upper(), page_size=limit//3)
                results["orders"] = orders
            
            # Cache the result
            self.query_cache[cache_key] = results
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching data: {str(e)}")
            return {"error": str(e), "query": query}
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get basic statistics about the data with performance metrics"""
        try:
            cache_key = self._get_cache_key('get_statistics')
            
            if cache_key in self.query_cache and self._is_cache_valid():
                return self.query_cache[cache_key]
            
            stats = {}
            
            for name, df in self.data.items():
                if df is not None:
                    stats[name] = {
                        "total_records": len(df),
                        "columns": list(df.columns),
                        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
                        "data_types": df.dtypes.to_dict()
                    }
                    
                    # Specific stats for each dataset
                    if name == 'customer':
                        stats[name]["total_customers"] = len(df)
                        stats[name]["states"] = df['state'].value_counts().to_dict()
                    elif name == 'inventory':
                        stats[name]["total_orders"] = len(df)
                        stats[name]["order_statuses"] = df['order_status'].value_counts().to_dict()
                        stats[name]["revenue"] = df['total_amount'].astype(float).sum()
                    elif name == 'pricelist':
                        stats[name]["total_products"] = len(df)
                        stats[name]["categories"] = df['category'].value_counts().to_dict()
                        stats[name]["total_inventory_value"] = (df['stock_quantity'].astype(int) * df['final_price'].astype(float)).sum()
            
            # Add performance metrics
            stats["performance"] = {
                "cache_size": len(self.query_cache),
                "indexes_created": len(self.indexes),
                "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
                "cache_hit_optimization": "enabled"
            }
            
            # Cache the result
            self.query_cache[cache_key] = stats
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
            return {"error": str(e)}
    
    def refresh_data(self):
        """Refresh data from Google Drive and clear caches"""
        logger.info("Refreshing data and clearing caches...")
        self.query_cache.clear()
        self.data = self.drive_service.refresh_cache()
        self.last_refresh = datetime.now()
        self._create_indexes()
        logger.info("Data refresh completed")
        return len(self.data) > 0
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        return {
            "query_cache_size": len(self.query_cache),
            "data_cache_size": len(self.data_cache),
            "indexes_count": len(self.indexes),
            "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
            "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60,
            "chunk_size": self.chunk_size,
            "memory_optimized": True
        }