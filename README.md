## SMRT Inventory Bot

A comprehensive mobile and web application that provides AI-powered insights into inventory listed in CSV through natural language queries.

### Key Features

- **Natural Language Queries**: Ask questions about customers, orders, and inventory in plain English
- **AI-Powered Responses**: Uses Google Gemini AI for intelligent query understanding
- **Real-time Data**: Integrates with Google Drive for live CSV data access
- **Scalable Architecture**: Optimized for large datasets with caching and indexing
- **Cross-Platform**: React Native mobile app + web dashboard

### Technologies Used

- **Backend**: Python, FastAPI, Pandas, Google Drive API, Google Gemini AI
- **Mobile**: React Native, Expo
- **Web**: React, Material-UI
- **Data**: Google Drive, CSV files

### Demo Queries

```
"How many customers do we have?"
"Name all the customers"
"Tell me about John"
"What items do we have in our inventory?"
"How many headphones do we have?"
"Generate a sales report"
```

### Architecture Highlights

- **Data-Grounded Responses**: All AI responses are validated against actual CSV data
- **Performance Optimization**: LRU caching, data indexing, and query optimization
- **Scalability**: Handles large CSV files with pagination and background processing
- **Error Handling**: Robust fallback mechanisms and data validation