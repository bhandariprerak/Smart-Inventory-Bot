from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Request/Response models
class ChatMessage(BaseModel):
    message: str
    user_id: Optional[str] = "default_user"

class ChatResponse(BaseModel):
    response: str
    status: str
    query_type: Optional[str] = None
    data_found: bool = False

class ReportRequest(BaseModel):
    report_type: str
    format: str = "text"

class ReportResponse(BaseModel):
    report: Dict[str, Any]
    generated_at: str
    report_type: str

# Initialize services
csv_processor = None
gemini_service = None
reports_service = None

try:
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    logger.info(f"Environment loaded - Service account file: {os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')}")
    logger.info(f"Environment loaded - Folder ID: {os.getenv('GOOGLE_DRIVE_FOLDER_ID')}")
    
    from app.services.csv_processor import ScalableCSVProcessor
    csv_processor = ScalableCSVProcessor()
    logger.info("Scalable CSV Processor initialized successfully")
    
    from app.services.gemini_ai import GeminiAIService
    gemini_service = GeminiAIService()
    if gemini_service.is_available():
        logger.info("Gemini AI Service initialized successfully")
    else:
        logger.warning("Gemini AI Service not available - check API key")
    
    try:
        from app.services.reports import VisualReportsService
        if csv_processor:
            reports_service = VisualReportsService(csv_processor)
            logger.info("Visual Reports Service initialized successfully")
    except ImportError:
        logger.warning("Visual Reports Service not available")
    
except Exception as e:
    logger.error(f"Failed to initialize services: {str(e)}")
    import traceback
    logger.error(traceback.format_exc())
    csv_processor = None
    gemini_service = None
    reports_service = None


class AIFirstQueryProcessor:
    """AI-first query processing with dynamic data access"""
    
    def __init__(self, csv_processor, gemini_service):
        self.csv_processor = csv_processor
        self.gemini_service = gemini_service
        self._customer_cache = {}
        self._product_cache = {}
    
    def get_all_customer_names(self) -> List[str]:
        """Dynamically get all customer names from current data"""
        try:
            if 'customer_names' not in self._customer_cache:
                customers = self.csv_processor.get_customers()
                full_names = [c['customer_name'] for c in customers]
                first_names = [name.split()[0].lower() for name in full_names]
                self._customer_cache['customer_names'] = first_names
                self._customer_cache['full_names'] = full_names
            
            return self._customer_cache['customer_names']
        except Exception as e:
            logger.error(f"Error getting customer names: {str(e)}")
            return []
    
    def get_all_product_terms(self) -> List[str]:
        """Dynamically get all product terms from current data"""
        try:
            if 'product_terms' not in self._product_cache:
                products = self.csv_processor.get_products()
                product_words = set()
                
                for product in products:
                    # Extract individual words from product names
                    words = product['product_name'].lower().split()
                    product_words.update(words)
                
                self._product_cache['product_terms'] = list(product_words)
            
            return self._product_cache['product_terms']
        except Exception as e:
            logger.error(f"Error getting product terms: {str(e)}")
            return []
    
    def is_known_customer(self, name: str) -> bool:
        """Check if customer exists in current database"""
        known_names = self.get_all_customer_names()
        return name.lower() in known_names
    
    def analyze_with_ai(self, user_message: str) -> Dict[str, Any]:
        """Use AI to understand query intent with dynamic context"""
        try:
            # Get current data context
            stats = self.csv_processor.get_statistics()
            customer_names = self.get_all_customer_names()
            product_terms = self.get_all_product_terms()
            
            # Enhanced context for AI
            enhanced_context = f"""
            Current Database Context:
            - Total Customers: {stats.get('customer', {}).get('total_records', 0)}
            - Customer Names: {', '.join(customer_names[:10])}{'...' if len(customer_names) > 10 else ''}
            - Total Orders: {stats.get('inventory', {}).get('total_records', 0)}
            - Order Statuses: {stats.get('inventory', {}).get('order_statuses', {})}
            - Total Products: {stats.get('pricelist', {}).get('total_records', 0)}
            - Product Categories: {stats.get('pricelist', {}).get('categories', {})}
            - Available Product Terms: {', '.join(product_terms[:15])}{'...' if len(product_terms) > 15 else ''}
            """
            
            analysis = self.gemini_service.analyze_query(user_message, enhanced_context)
            
            # Add dynamic entity extraction
            analysis['dynamic_entities'] = self.extract_dynamic_entities(user_message)
            
            return analysis
            
        except Exception as e:
            logger.error(f"AI analysis failed: {str(e)}")
            return {"error": str(e)}
    
    def extract_dynamic_entities(self, user_message: str) -> Dict[str, Any]:
        """Extract entities dynamically from current data"""
        entities = {
            "customer_names": [],
            "unknown_customers": [],
            "product_terms": [],
            "order_statuses": [],
            "numbers": []
        }
        
        user_lower = user_message.lower()
        words = user_message.split()
        
        # Dynamic customer name extraction
        known_customers = self.get_all_customer_names()
        for name in known_customers:
            if name in user_lower:
                entities["customer_names"].append(name.capitalize())
        
        # Extract unknown customers
        for word in words:
            if (word.lower() not in known_customers and 
                len(word) > 2 and 
                word.isalpha() and 
                word.lower() not in ['customer', 'have', 'orders', 'tell', 'about']):
                entities["unknown_customers"].append(word)
        
        # Dynamic product term extraction
        product_terms = self.get_all_product_terms()
        for term in product_terms:
            if term in user_lower and len(term) > 3:  # Avoid small words like "pc", "tv"
                entities["product_terms"].append(term)
        
        # Order status extraction
        order_statuses = ['delivered', 'shipped', 'processing', 'cancelled']
        for status in order_statuses:
            if status in user_lower:
                entities["order_statuses"].append(status.capitalize())
        
        return entities
    
    def execute_query_from_ai_intent(self, analysis: Dict, user_message: str) -> Dict[str, Any]:
        """Execute database queries based on AI analysis"""
        try:
            intent = analysis.get("intent", "")
            action = analysis.get("action", "")
            filters = analysis.get("filters", {})
            entities = analysis.get("dynamic_entities", {})
            
            result = {"data": [], "message": "", "query_executed": ""}
            
            # Customer-related queries
            if intent == "customers" or "customer" in intent:
                if action == "count" or "how many" in user_message.lower():
                    customers = self.csv_processor.get_customers()
                    result["data"] = len(customers)
                    result["message"] = f"We have {len(customers)} customers in total."
                    result["query_executed"] = "customer_count"
                
                elif action == "list" or "names" in user_message.lower():
                    customers = self.csv_processor.get_customers()
                    names = [c['customer_name'] for c in customers]
                    result["data"] = names
                    result["message"] = f"Our customer names are: {', '.join(names)}"
                    result["query_executed"] = "customer_list"
                
                elif entities["customer_names"]:
                    # Known customer query
                    customer_name = entities["customer_names"][0]
                    customers = self.csv_processor.get_customers(customer_name=customer_name)
                    if customers:
                        customer = customers[0]
                        result["data"] = customer
                        result["message"] = f"{customer['customer_name']}: Email: {customer['email']}, Phone: {customer['phone']}, City: {customer['city']}, State: {customer['state']}"
                        result["query_executed"] = "customer_info"
                
                elif entities["unknown_customers"]:
                    # Unknown customer query
                    unknown_name = entities["unknown_customers"][0]
                    customers = self.csv_processor.get_customers()
                    all_names = [c['customer_name'] for c in customers]
                    result["data"] = {"unknown_name": unknown_name, "all_customers": all_names}
                    result["message"] = f"No customer named '{unknown_name}' found. Our customers are: {', '.join(all_names)}"
                    result["query_executed"] = "unknown_customer"
            
            # Order-related queries
            elif intent == "orders" or "order" in intent:
                if entities["customer_names"]:
                    # Customer-specific orders
                    customer_name = entities["customer_names"][0]
                    order_data = self.get_customer_orders_detailed(customer_name)
                    result["data"] = order_data
                    if "error" not in order_data and order_data.get("orders"):
                        orders = order_data["orders"]
                        order_summaries = []
                        for order in orders:
                            products_text = ", ".join([f"{p['product_name']} (x{p['quantity']})" for p in order['products']])
                            order_summaries.append(f"Order #{order['order_id']} on {order['order_date']}: {products_text}, Total: ${order['total_amount']}, Status: {order['status']}")
                        result["message"] = f"{order_data['customer']['customer_name']} has {len(orders)} orders: " + "; ".join(order_summaries)
                        result["query_executed"] = "customer_orders"
                    else:
                        result["message"] = f"No orders found for customer '{customer_name}'"
                
                elif "how many" in user_message.lower():
                    # Order count queries
                    if entities["order_statuses"]:
                        status = entities["order_statuses"][0]
                        orders = self.csv_processor.get_orders(status=status)
                        result["data"] = len(orders)
                        result["message"] = f"We have {len(orders)} {status.lower()} orders."
                        result["query_executed"] = "order_count_by_status"
                    else:
                        orders = self.csv_processor.get_orders()
                        result["data"] = len(orders)
                        result["message"] = f"We have {len(orders)} orders in total."
                        result["query_executed"] = "total_order_count"
                
                else:
                    # General order information
                    orders = self.csv_processor.get_orders()
                    status_counts = {}
                    for order in orders:
                        status = order['order_status']
                        status_counts[status] = status_counts.get(status, 0) + 1
                    
                    result["data"] = {"total": len(orders), "by_status": status_counts}
                    result["message"] = f"We have {len(orders)} total orders. Status breakdown: {status_counts}"
                    result["query_executed"] = "order_summary"
            
            # Product/Inventory queries
            elif intent == "products" or intent == "inventory":
                if "how many" in user_message.lower() and entities["product_terms"]:
                    # Product count query
                    products = self.search_products_by_terms(entities["product_terms"])
                    if products:
                        total_stock = sum(int(p['stock_quantity']) for p in products)
                        if len(products) == 1:
                            result["message"] = f"We have {products[0]['stock_quantity']} {products[0]['product_name']} in stock."
                        else:
                            product_details = [f"{p['stock_quantity']} {p['product_name']}" for p in products]
                            result["message"] = f"We have: {', '.join(product_details)} (Total: {total_stock} units)"
                        result["data"] = products
                        result["query_executed"] = "product_count"
                    else:
                        result["message"] = f"We don't have any products matching those terms."
                
                elif any(phrase in user_message.lower() for phrase in ['do we have', 'is there']):
                    # Product existence query
                    products = self.search_products_by_terms(entities["product_terms"])
                    if products:
                        if len(products) == 1:
                            result["message"] = f"Yes, we have {products[0]['stock_quantity']} {products[0]['product_name']} in stock at ${products[0]['final_price']} each."
                        else:
                            product_names = [p['product_name'] for p in products]
                            result["message"] = f"Yes, we have these matching products: {', '.join(product_names)}"
                        result["data"] = products
                        result["query_executed"] = "product_existence"
                    else:
                        result["message"] = f"No, we don't have products matching those terms in our inventory."
                
                else:
                    # General inventory status
                    products = self.csv_processor.get_products()
                    inventory_summary = [f"{p['stock_quantity']} {p['product_name']}" for p in products]
                    result["data"] = products
                    result["message"] = f"Current inventory: {', '.join(inventory_summary)}"
                    result["query_executed"] = "inventory_status"
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return {"error": str(e), "message": "Error processing your request"}
    
    def search_products_by_terms(self, terms: List[str]) -> List[Dict]:
        """Search products using dynamic terms"""
        try:
            all_products = self.csv_processor.get_products()
            matching_products = []
            
            for product in all_products:
                product_name_lower = product['product_name'].lower()
                
                for term in terms:
                    if term.lower() in product_name_lower:
                        matching_products.append(product)
                        break
            
            return matching_products
        except Exception as e:
            logger.error(f"Error searching products: {str(e)}")
            return []
    
    def get_customer_orders_detailed(self, customer_name: str) -> Dict[str, Any]:
        """Get detailed customer orders"""
        try:
            customers = self.csv_processor.get_customers(customer_name=customer_name)
            if not customers:
                return {"error": "Customer not found"}
            
            customer = customers[0]
            orders = self.csv_processor.get_orders(customer_id=customer['CID'])
            
            if not orders:
                return {"customer": customer, "orders": [], "message": f"{customer['customer_name']} hasn't placed any orders yet."}
            
            detailed_orders = []
            for order in orders:
                details = self.csv_processor.get_order_details(order_id=order['IID'])
                products = []
                
                for detail in details:
                    all_products = self.csv_processor.get_products()
                    product = next((p for p in all_products if p['price_table_item_id'] == detail['price_table_item_id']), None)
                    if product:
                        products.append({
                            "product_name": product['product_name'],
                            "quantity": detail['quantity']
                        })
                
                detailed_orders.append({
                    "order_id": order['IID'],
                    "order_date": order['order_date'],
                    "status": order['order_status'],
                    "total_amount": order['total_amount'],
                    "products": products
                })
            
            return {"customer": customer, "orders": detailed_orders}
        except Exception as e:
            logger.error(f"Error getting customer orders: {str(e)}")
            return {"error": str(e)}


# Initialize AI-first processor
ai_processor = None
if csv_processor and gemini_service:
    ai_processor = AIFirstQueryProcessor(csv_processor, gemini_service)
    logger.info("AI-first query processor initialized successfully")


@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(chat_message: ChatMessage):
    """
    AI-first chat endpoint with dynamic processing
    """
    try:
        user_message = chat_message.message.strip()
        logger.info(f"Received message: {user_message}")
        
        if not user_message:
            raise HTTPException(status_code=400, detail="Message cannot be empty")
        
        if not ai_processor:
            return ChatResponse(
                response="AI processing system is not available.",
                status="error",
                query_type="error",
                data_found=False
            )
        
        # Handle simple greetings without AI
        if any(word in user_message.lower() for word in ['hi', 'hello', 'hey']):
            return ChatResponse(
                response="Hello! I'm your SMRT Inventory Bot. I can help you with customer information, order history, and product inventory. What would you like to know?",
                status="success",
                query_type="greeting",
                data_found=True
            )
        
        # Use AI to analyze the query
        # Use AI to analyze the query
        ai_analysis = ai_processor.analyze_with_ai(user_message)
        
        #### me test
        if "error" in ai_analysis:
            logger.warning(f"AI analysis failed, using comprehensive fallback: {ai_analysis['error']}")
            
            # Comprehensive fallback without AI
            user_lower = user_message.lower()
            
            # Customer count
            if "customer" in user_lower and ("how many" in user_lower or "count" in user_lower):
                customers = csv_processor.get_customers()
                return ChatResponse(response=f"We have {len(customers)} customers in total.", status="success", query_type="customer_count", data_found=True)
            
            # Customer list
            elif "customer" in user_lower and ("names" in user_lower or "all" in user_lower):
                customers = csv_processor.get_customers()
                names = [c['customer_name'] for c in customers]
                return ChatResponse(response=f"Our customer names are: {', '.join(names)}", status="success", query_type="customer_list", data_found=True)
            
            # Specific customer info (tell me about X, X details)
            elif any(phrase in user_lower for phrase in ['tell me about', 'details about', 'info about']):
                # Extract customer name dynamically
                customers = csv_processor.get_customers()
                customer_names = [c['customer_name'].split()[0].lower() for c in customers]
                
                found_customer = None
                for name in customer_names:
                    if name in user_lower:
                        found_customer = name
                        break
                
                if found_customer:
                    customers_data = csv_processor.get_customers(customer_name=found_customer.capitalize())
                    if customers_data:
                        customer = customers_data[0]
                        return ChatResponse(response=f"{customer['customer_name']}: Email: {customer['email']}, Phone: {customer['phone']}, City: {customer['city']}, State: {customer['state']}", status="success", query_type="customer_info", data_found=True)
                else:
                    return ChatResponse(response="Please specify which customer you want to know about. Our customers are: " + ", ".join([c['customer_name'] for c in customers]), status="success", query_type="customer_help", data_found=True)
            
            # Product count (how many X)
            elif "how many" in user_lower:
                products = csv_processor.get_products()
                for product in products:
                    product_words = product['product_name'].lower().split()
                    if any(word in user_lower for word in product_words):
                        return ChatResponse(response=f"We have {product['stock_quantity']} {product['product_name']} in stock.", status="success", query_type="product_count", data_found=True)
                return ChatResponse(response="I couldn't find that product. Try asking about specific items like 'headphones', 'speakers', or 'mouse'.", status="success", query_type="product_help", data_found=True)
            
            # Unknown customer check
            elif "is" in user_lower and "customer" in user_lower:
                customers = csv_processor.get_customers()
                known_names = [c['customer_name'].split()[0].lower() for c in customers]
                
                # Check if it's a known customer
                found_name = None
                for name in known_names:
                    if name in user_lower:
                        found_name = name
                        break
                
                if found_name:
                    customers_data = csv_processor.get_customers(customer_name=found_name.capitalize())
                    if customers_data:
                        customer = customers_data[0]
                        return ChatResponse(response=f"Yes, {customer['customer_name']}: Email: {customer['email']}, Phone: {customer['phone']}", status="success", query_type="customer_info", data_found=True)
                else:
                    # Unknown customer
                    all_names = [c['customer_name'] for c in customers]
                    return ChatResponse(response=f"I don't see that customer in our database. Our customers are: {', '.join(all_names)}", status="success", query_type="unknown_customer", data_found=True)
            
            # Inventory status
            elif "inventory" in user_lower:
                products = csv_processor.get_products()
                inventory_summary = [f"{p['stock_quantity']} {p['product_name']}" for p in products]
                return ChatResponse(response=f"Current inventory: {', '.join(inventory_summary)}", status="success", query_type="inventory_status", data_found=True)
            
            # Default fallback
            else:
                return ChatResponse(response="I can help with: customer information ('tell me about John'), customer counts ('how many customers'), inventory status ('what does inventory look like'), and product quantities ('how many headphones').", status="success", query_type="fallback", data_found=False)

        #### end test
        
        # Execute query based on AI analysis
        query_result = ai_processor.execute_query_from_ai_intent(ai_analysis, user_message)
        
        if "error" in query_result:
            logger.error(f"Query execution failed: {query_result['error']}")
            return ChatResponse(
                response="Sorry, I encountered an error while processing your request. Please try again.",
                status="error",
                query_type="error",
                data_found=False
            )
        
        # Generate response using AI if available
        if gemini_service and gemini_service.is_available():
            try:
                ai_response = gemini_service.generate_response(ai_analysis, query_result["data"], user_message)
                if ai_response and len(ai_response.strip()) > 5:
                    response_text = ai_response
                else:
                    response_text = query_result["message"]
            except:
                response_text = query_result["message"]
        else:
            response_text = query_result["message"]
        
        return ChatResponse(
            response=response_text,
            status="success",
            query_type=query_result.get("query_executed", "ai_processed"),
            data_found=bool(query_result.get("data"))
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


# REPORT ENDPOINTS (unchanged)
@router.get("/reports/dashboard")
async def get_dashboard_data():
    """Get comprehensive dashboard data"""
    if reports_service is None:
        raise HTTPException(status_code=500, detail="Reports service not available")
    
    try:
        dashboard_data = reports_service.generate_dashboard_data()
        return {
            "data": dashboard_data,
            "generated_at": datetime.now().isoformat(),
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error generating dashboard data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Dashboard generation failed: {str(e)}")


@router.get("/reports/text/{report_type}")
async def get_text_report(report_type: str):
    """Generate text reports"""
    if reports_service is None:
        raise HTTPException(status_code=500, detail="Reports service not available")
    
    valid_types = ["executive_summary", "customer_report", "sales_report", "inventory_report"]
    if report_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid report type. Valid: {valid_types}")
    
    try:
        report_text = reports_service.generate_text_report(report_type)
        return {
            "report": report_text,
            "report_type": report_type,
            "format": "text",
            "generated_at": datetime.now().isoformat(),
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error generating text report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")


@router.get("/test")
async def test_endpoint():
    """Test endpoint"""
    return {
        "message": "SMRT Inventory Bot API is operational with AI-first processing!",
        "services": {
            "csv_processor": "initialized" if csv_processor is not None else "error",
            "gemini_ai": "available" if gemini_service and gemini_service.is_available() else "not_available",
            "ai_processor": "available" if ai_processor is not None else "not_available",
            "reports": "available" if reports_service is not None else "not_available"
        },
        "features": {
            "dynamic_customer_detection": "enabled",
            "ai_intent_analysis": "enabled",
            "scalable_queries": "enabled",
            "no_hardcoded_values": "enabled"
        }
    }


@router.get("/data-status")
async def data_status():
    """System status check"""
    try:
        if csv_processor is None:
            return {"error": "CSV processor not initialized", "status": "error"}
        
        stats = csv_processor.get_statistics()
        csv_status = {}
        
        for name in ['customer', 'inventory', 'detail', 'pricelist']:
            if name in stats:
                csv_status[name] = f"loaded ({stats[name]['total_records']} records)"
            else:
                csv_status[name] = "not_loaded"
        
        # Get dynamic data info
        dynamic_info = {}
        if ai_processor:
            customer_names = ai_processor.get_all_customer_names()
            product_terms = ai_processor.get_all_product_terms()
            dynamic_info = {
                "dynamic_customers": len(customer_names),
                "dynamic_products": len(product_terms),
                "sample_customers": customer_names[:5],
                "sample_products": product_terms[:5]
            }
        
        return {
            "csv_files": csv_status,
            "services": {
                "google_drive": "connected" if len(stats) > 0 else "error",
                "gemini_ai": "available" if gemini_service and gemini_service.is_available() else "not_configured",
                "ai_processor": "available" if ai_processor is not None else "not_configured"
            },
            "dynamic_data": dynamic_info,
            "status": "fully_operational" if len(stats) > 0 and gemini_service and ai_processor else "partial_service",
            "architecture": "ai_first_dynamic_scalable"
        }
    except Exception as e:
        logger.error(f"Error in data_status: {str(e)}")
        return {"error": str(e), "status": "error"}