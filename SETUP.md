## Setup Instructions

### Prerequisites

1. **Google Cloud Account** with:
   - Google Drive API enabled
   - Google Gemini API enabled
   - Service account with JSON credentials

2. **Development Environment**:
   - Node.js 16+ 
   - Python 3.12 # recommended version for Pandas library
   - Docker

### Quick Start

1. **Clone the repository**:
```bash
git clone https://github.com/yourusername/ai-finance-assistant
cd ai-finance-assistant
```

2. **Set up environment variables**:
```bash
cp .env.example .env
# Edit .env with your Google API credentials
```

3. **Place service account JSON**:
```bash
# Place your Google service account JSON in:
backend/credentials/service-account.json
```

4. **Upload sample data to Google Drive**:
```bash
# Upload the CSV files from /data folder to a Google Drive folder
# Share the folder with your service account email
# Update GOOGLE_DRIVE_FOLDER_ID in .env
# Kindly share the folder with Read access to your Google project's client email
```

5. **Start the application**:
#### Backend Setup

```bash
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Set environment variables
export GOOGLE_SERVICE_ACCOUNT_FILE=credentials/service-account.json
export GOOGLE_DRIVE_FOLDER_ID=your-folder-id
export GEMINI_API_KEY=your-gemini-api-key
export REACT_APP_API_IP=your-local-ip-address

# Run the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

#### Frontend Setup

```bash
npm install
npx expo start
```
#### Flow
```
User Input → Intent Classification → Data Retrieval → AI Processing → Validation → Response
```

#### Data Validation Layers

- **Schema Validation**: All CSV data is validated against predefined schemas
- **Cross-Reference Checks**: Orders are validated against customers and products
- **Cache Consistency**: Cached results are invalidated when source data changes
- **Response Verification**: AI responses are fact-checked against raw data

#### Hallucination Prevention

- **Grounded Generation**: AI only generates responses based on retrieved data
- **Fact Checking**: Numerical claims are verified against source data

### Scalability Feature

#### Data Processing Optimization

```python
# Example: Indexed customer lookup
class ScalableCSVProcessor:
    def _create_indexes(self):
        self.indexes = {
            'customer_by_name': {name: idx for idx, name in enumerate(names)},
            'orders_by_customer': defaultdict(list),
            'products_by_category': defaultdict(list)
        }
    
    @lru_cache(maxsize=1000)
    def get_customers(self, name=None, page=1, page_size=100):
        # O(1) lookup using indexes instead of O(n) scan
        return self.cached_query(name, page, page_size)
```