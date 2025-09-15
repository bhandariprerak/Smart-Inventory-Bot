import os
import json
import requests
from typing import Dict, Any, Optional
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class GeminiAIService:
    def __init__(self):
        self.api_key = os.getenv('GEMINI_API_KEY')
        self.base_url = "https://generativelanguage.googleapis.com/v1beta"
        self.model = "gemini-1.5-flash-latest"
        
        if not self.api_key or self.api_key == "your_gemini_api_key_here":
            logger.warning("Gemini API key not configured properly")
            self.api_key = None
    
    def is_available(self) -> bool:
        """Check if Gemini AI is properly configured"""
        return self.api_key is not None and self.api_key != "your_gemini_api_key_here"
    
    def analyze_query(self, user_message: str, data_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze user query to determine intent and extract parameters
        """
        if not self.is_available():
            return {"error": "Gemini AI not configured"}
        
        # Create context about available data
        context = f"""
        Available Data:
        - Customers: {data_context.get('customer', {}).get('total_records', 0)} records
        - Orders: {data_context.get('inventory', {}).get('total_records', 0)} records  
        - Order Details: {data_context.get('detail', {}).get('total_records', 0)} records
        - Products: {data_context.get('pricelist', {}).get('total_records', 0)} records
        
        Order Statuses Available: {data_context.get('inventory', {}).get('order_statuses', {})}
        Product Categories: {data_context.get('pricelist', {}).get('categories', {})}
        """
        
        prompt = f"""
        You are an AI assistant analyzing user queries about inventory, customer information or orders from the files.
        
        {context}
        
        User Query: "{user_message}"
        
        Analyze this query and respond with a JSON object containing:
        {{
            "intent": "customers|orders|products|general|count|specific_person",
            "action": "list|search|count|filter",
            "filters": {{
                "customer_name": "name if specified",
                "product_category": "category if specified", 
                "order_status": "status if specified",
                "product_name": "product if specified"
            }},
            "response_type": "summary|detailed|count",
            "confidence": 0.0-1.0
        }}
        
        Examples:
        - "Show me customers" → {{"intent": "customers", "action": "list", "response_type": "summary"}}
        - "Tell me about John" → {{"intent": "specific_person", "action": "search", "filters": {{"customer_name": "John"}}, "response_type": "detailed"}}
        - "How many delivered orders?" → {{"intent": "orders", "action": "count", "filters": {{"order_status": "delivered"}}, "response_type": "count"}}
        - "What electronics do we sell?" → {{"intent": "products", "action": "filter", "filters": {{"product_category": "Electronics"}}, "response_type": "summary"}}
        
        Respond ONLY with valid JSON, no other text.
        """
        
        try:
            response = self._call_gemini(prompt)
            
            # Try to parse JSON from response
            if response and 'candidates' in response:
                content = response['candidates'][0]['content']['parts'][0]['text']
                
                # Clean up the response to extract JSON
                content = content.strip()
                if content.startswith('```json'):
                    content = content[7:-3]
                elif content.startswith('```'):
                    content = content[3:-3]
                
                try:
                    analysis = json.loads(content)
                    return analysis
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}, Content: {content}")
                    return {"error": "Invalid JSON response from Gemini"}
            
            return {"error": "No valid response from Gemini"}
            
        except Exception as e:
            logger.error(f"Error analyzing query with Gemini: {str(e)}")
            return {"error": str(e)}
    
    def generate_response(self, query_analysis: Dict[str, Any], data_results: Any, user_message: str) -> str:
        """
        Generate natural language response based on query analysis and data results
        """
        if not self.is_available():
            return "Gemini AI not available for response generation."
        
        prompt = f"""
        You are a helpful AI Inventory Bot. Generate a natural, conversational response.
        
        User asked: "{user_message}"
        Query Analysis: {json.dumps(query_analysis)}
        Data Results: {json.dumps(data_results, default=str) if data_results else "No data found"}
        
        Generate a helpful, natural response. Be specific about the data you found.
        If data is empty or None, explain that no matching records were found.
        Keep responses conversational but informative.
        
        Examples of good responses:
        - "I found 3 customers in the database: John Smith, Sarah Johnson, and Mike Wilson."
        - "John Smith's details: Email is john.smith@email.com, Phone is 555-0101, and he lives in New York, NY."
        - "We have 5 delivered orders out of 12 total orders."
        - "In Electronics category, we sell: Wireless Headphones ($149.99), Bluetooth Speaker ($149.50), and Gaming Mouse ($279.99)."
        
        Respond with ONLY the response text, no JSON or extra formatting.
        """
        
        try:
            response = self._call_gemini(prompt)
            
            if response and 'candidates' in response:
                content = response['candidates'][0]['content']['parts'][0]['text']
                return content.strip()
            
            return "I couldn't generate a proper response. Please try rephrasing your question."
            
        except Exception as e:
            logger.error(f"Error generating response with Gemini: {str(e)}")
            return f"Error generating response: {str(e)}"
    
    def _call_gemini(self, prompt: str) -> Optional[Dict]:
        """Make API call to Gemini"""
        url = f"{self.base_url}/models/{self.model}:generateContent"
        
        headers = {
            'Content-Type': 'application/json',
        }
        
        data = {
            "contents": [{
                "parts": [{
                    "text": prompt
                }]
            }],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 1000,
            }
        }
        
        try:
            response = requests.post(
                f"{url}?key={self.api_key}",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Gemini API error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error calling Gemini API: {str(e)}")
            return None