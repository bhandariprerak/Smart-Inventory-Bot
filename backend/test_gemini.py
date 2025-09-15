#!/usr/bin/env python3

from dotenv import load_dotenv
load_dotenv()

try:
    from app.services.gemini_ai import GeminiAIService
    
    print("=== Testing Gemini AI Service ===")
    
    gemini = GeminiAIService()
    print(f"Gemini available: {gemini.is_available()}")
    
    if gemini.is_available():
        # Test query analysis
        test_queries = [
            "tell me their names",
            "do we have Jessica as a customer",
            "tell me about Sarth", 
            "what are all customer names",
            "show me everyone"
        ]
        
        # Mock data context
        context = {
            "customer": {"total_records": 10},
            "inventory": {"total_records": 12, "order_statuses": {"Delivered": 5}},
            "pricelist": {"total_records": 8, "categories": {"Electronics": 6, "Office": 2}}
        }
        
        for query in test_queries:
            print(f"\nQuery: '{query}'")
            analysis = gemini.analyze_query(query, context)
            print(f"Analysis: {analysis}")
    else:
        print("Gemini not available - check API key")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()