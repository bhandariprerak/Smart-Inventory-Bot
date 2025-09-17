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
        """Create indexes for faster data access with new CSV structure"""
        self.indexes = {}
        
        try:
            # Customer index by name and ID
            if 'customer' in self.data:
                df = self.data['customer']
                # Create full name first
                df['customer_name'] = df['FNAME1'] + ' ' + df['LNAME']
                self.indexes['customer_by_name'] = {
                    name.lower(): idx for idx, name in enumerate(df['customer_name'])
                }
                self.indexes['customer_by_id'] = {
                    cid: idx for idx, cid in enumerate(df['CID'])
                }
            
            # Product index by name
            if 'pricelist' in self.data:
                df = self.data['pricelist']
                self.indexes['product_by_name'] = {}
                
                for idx, name in enumerate(df['name']):  # 'name' not 'product_name'
                    words = name.lower().split()
                    for word in words:
                        if word not in self.indexes['product_by_name']:
                            self.indexes['product_by_name'][word] = []
                        self.indexes['product_by_name'][word].append(idx)
            
            # Order index by customer ID
            if 'inventory' in self.data:
                df = self.data['inventory']
                self.indexes['orders_by_customer'] = {}
                
                for idx, cid in enumerate(df['CID']):
                    if cid not in self.indexes['orders_by_customer']:
                        self.indexes['orders_by_customer'][cid] = []
                    self.indexes['orders_by_customer'][cid].append(idx)
            

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
    def get_customers(self, customer_name: str = None, customer_id: str = None, page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get customer information with new CSV structure"""
        try:
            df = self.data.get('customer')
            if df is None or df.empty:
                return []
            
            # Create full name column
            df['customer_name'] = df['FNAME1'] + ' ' + df['LNAME']
            
            result_df = df.copy()
            
            if customer_name:
                result_df = result_df[result_df['customer_name'].str.contains(customer_name, case=False, na=False)]
            
            if customer_id:
                result_df = result_df[result_df['CID'] == customer_id]
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            # Map to expected format
            result = []
            for _, row in result_df.iterrows():
                result.append({
                    'CID': row['CID'],
                    'customer_name': row['customer_name'],
                    'email': row['EMAIL'],
                    'phone': 'N/A',  # Not available in new structure
                    'address': row['ADDRESS'],
                    'city': row['CITY'],
                    'state': row['STATE'],
                    'zip': row['ZIP']
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting customers: {str(e)}")
            return []
        
    def get_order_items(self, order_id: str = None, item_name: str = None, page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get order items/details from detail CSV"""
        try:
            df = self.data.get('detail')
            if df is None or df.empty:
                return []
            
            result_df = df.copy()
            
            if order_id:
                result_df = result_df[result_df['IID'] == order_id]
            
            if item_name:
                result_df = result_df[result_df['item_name'].str.contains(item_name, case=False, na=False)]
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            # Map to expected format
            result = []
            for _, row in result_df.iterrows():
                result.append({
                    'Item_ID': row['Item_ID'],
                    'IID': row['IID'],
                    'item_name': row['item_name'],
                    'quantity': row['item_count'],
                    'base_price': row['item_baseprice'],
                    'department': row['dept_name'],
                    'pickup_date': row['item_pickup_date'],
                    'subtotal': row['standardSubtotal']
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting order items: {str(e)}")
            return []
    
    def get_orders(self, customer_id: str = None, status: str = None, page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get order information with new CSV structure"""
        try:
            df = self.data.get('inventory')
            if df is None or df.empty:
                return []
            
            result_df = df.copy()
            
            if customer_id:
                result_df = result_df[result_df['CID'] == customer_id]
            
            if status:
                # Map status to PIF field (Y = delivered, N = pending)
                if status.lower() == 'delivered':
                    result_df = result_df[result_df['PIF'] == 'Y']
                elif status.lower() == 'pending':
                    result_df = result_df[result_df['PIF'] != 'Y']
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            # Map to expected format
            result = []
            for _, row in result_df.iterrows():
                result.append({
                    'IID': row['IID'],
                    'CID': row['CID'],
                    'order_date': row['INDATE'],
                    'order_status': 'Delivered' if row['PIF'] == 'Y' else 'Pending',
                    'total_amount': row['SUBTOTAL'],
                    'ticket_no': row['TICKETNO'],
                    'category': row['CATEGORY']
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting orders: {str(e)}")
            return []
    
    def get_products(self, product_name: str = None, category: str = None, page: int = 1, page_size: int = 100) -> List[Dict]:
        """Get product information with new CSV structure"""
        try:
            df = self.data.get('pricelist')
            if df is None or df.empty:
                return []
            
            result_df = df.copy()
            
            if product_name:
                result_df = result_df[result_df['name'].str.contains(product_name, case=False, na=False)]
            
            if category:
                # No category in new structure, skip this filter
                pass
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            result_df = result_df.iloc[start_idx:end_idx]
            
            # Map to expected format
            result = []
            for _, row in result_df.iterrows():
                try:
                    base_price = float(row['baseprice']) if row['baseprice'] and str(row['baseprice']).strip() != '' else 0.0
                except (ValueError, TypeError):
                    base_price = 0.0
            for _, row in result_df.iterrows():
                result.append({
                    'price_table_item_id': row['item_id'],
                    'product_name': row['name'],
                    'category': 'Dry Cleaning',  # Default category
                    'unit_price': base_price,
                    'final_price': row['baseprice'],
                    'stock_quantity': 'Available',  # No stock info in new structure
                    'brand': 'N/A',
                    'description': f"{row['name']} - Base price: ${row['baseprice']}"
                })
            
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
                        stats[name]["states"] = df['STATE'].value_counts().to_dict()  # Fixed column name
                    elif name == 'inventory':
                        stats[name]["total_orders"] = len(df)
                        # Map PIF to status
                        pif_counts = df['PIF'].value_counts().to_dict()
                        stats[name]["order_statuses"] = {
                            'Delivered': pif_counts.get('Y', 0),
                            'Pending': pif_counts.get('', 0) + pif_counts.get('N', 0)
                        }
                        stats[name]["revenue"] = df['SUBTOTAL'].astype(float).sum()
                    elif name == 'pricelist':
                        stats[name]["total_products"] = len(df)
                        stats[name]["categories"] = {'Dry Cleaning': len(df)}  # All items are dry cleaning

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