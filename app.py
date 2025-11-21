from flask import Flask, request, jsonify
from flask_cors import CORS
import redis
import json
import sys
import os
import time

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Redis connection with environment variables for Docker
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Retry logic for Redis connection (important for Docker startup)
def get_redis_client():
    max_retries = 5
    for i in range(max_retries):
        try:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=0,
                decode_responses=True,
                max_connections=50,
                socket_connect_timeout=5
            )
            client.ping()
            print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except redis.ConnectionError as e:
            if i < max_retries - 1:
                print(f"⏳ Waiting for Redis... (attempt {i+1}/{max_retries})")
                time.sleep(2)
            else:
                print(f"❌ Failed to connect to Redis: {e}")
                raise

redis_client = get_redis_client()

# Redis key prefixes
CITY_PREFIX = "city:"
NAME_INDEX_PREFIX = "name:"
SEARCH_PREFIX = "search:"

def load_cities_to_redis(json_file='cities.json'):
    """Load cities from JSON/JSONL into Redis with multiple indexes"""
    print("Loading cities into Redis...")
    
    cities = []
    with open(json_file, 'r') as f:
        # Try to load as JSON array first
        try:
            f.seek(0)
            cities = json.load(f)
            print("Loaded as JSON array")
        except json.JSONDecodeError:
            # If that fails, treat as JSONL (one JSON object per line)
            print("Loading as JSONL format (one object per line)...")
            f.seek(0)
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        cities.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"⚠️  Skipping invalid JSON on line {line_num}: {e}")
                        continue
    
    pipe = redis_client.pipeline()
    batch_size = 1000
    
    for idx, city in enumerate(cities):
        city_id = city['id']
        name_lower = city['name'].lower()
        
        # Store full city data by ID
        pipe.set(f"{CITY_PREFIX}{city_id}", json.dumps(city))
        
        # Index by exact name (lowercase)
        pipe.sadd(f"{NAME_INDEX_PREFIX}{name_lower}", city_id)
        
        # Create prefix indexes for fast autocomplete (1-char to full name)
        for i in range(1, len(name_lower) + 1):
            prefix = name_lower[:i]
            pipe.sadd(f"{SEARCH_PREFIX}{prefix}", city_id)
        
        # Execute in batches to avoid memory issues
        if (idx + 1) % batch_size == 0:
            pipe.execute()
            pipe = redis_client.pipeline()
            print(f"Processed {idx + 1}/{len(cities)} cities...")
    
    # Execute remaining commands
    pipe.execute()
    print(f"✅ Loaded {len(cities)} cities into Redis")
    print(f"Total keys: {redis_client.dbsize()}")

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        redis_client.ping()
        return jsonify({
            'status': 'ok',
            'redis': 'connected',
            'redis_host': REDIS_HOST,
            'total_keys': redis_client.dbsize()
        })
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'redis': 'disconnected',
            'error': str(e)
        }), 503

@app.route('/search')
def search():
    """
    Search cities by name prefix
    Usage: /search?q=shang&limit=10
    """
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 10))
    
    if not query:
        return jsonify({'error': 'Query parameter "q" required'}), 400
    
    query_lower = query.lower()
    
    # Get city IDs from prefix index
    city_ids = redis_client.smembers(f"{SEARCH_PREFIX}{query_lower}")
    
    if not city_ids:
        return jsonify({
            'query': query,
            'count': 0,
            'results': []
        })
    
    # Fetch city data (use pipeline for batch fetch)
    pipe = redis_client.pipeline()
    for city_id in list(city_ids)[:limit]:
        pipe.get(f"{CITY_PREFIX}{city_id}")
    
    cities_data = pipe.execute()
    results = [json.loads(city) for city in cities_data if city]
    
    return jsonify({
        'query': query,
        'count': len(results),
        'results': results
    })

@app.route('/city/<int:city_id>')
def get_city(city_id):
    """
    Get city by ID
    Usage: /city/1796236
    """
    city_data = redis_client.get(f"{CITY_PREFIX}{city_id}")
    
    if not city_data:
        return jsonify({'error': 'City not found'}), 404
    
    return jsonify(json.loads(city_data))

@app.route('/autocomplete')
def autocomplete():
    """
    Fast autocomplete endpoint (returns only names)
    Usage: /autocomplete?q=sh&limit=10
    """
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 10))
    
    if not query:
        return jsonify({'error': 'Query parameter "q" required'}), 400
    
    query_lower = query.lower()
    city_ids = redis_client.smembers(f"{SEARCH_PREFIX}{query_lower}")
    
    # Fetch only names (faster than full objects)
    pipe = redis_client.pipeline()
    for city_id in list(city_ids)[:limit]:
        pipe.get(f"{CITY_PREFIX}{city_id}")
    
    cities_data = pipe.execute()
    suggestions = [json.loads(city)['name'] for city in cities_data if city]
    
    return jsonify({
        'query': query,
        'suggestions': suggestions
    })

@app.route('/')
def index():
    """API documentation"""
    return jsonify({
        'name': 'City Search API',
        'version': '1.0.0',
        'endpoints': {
            '/search': 'Search cities by name (e.g., /search?q=shanghai&limit=10)',
            '/city/<id>': 'Get city by ID (e.g., /city/1796236)',
            '/autocomplete': 'Fast name autocomplete (e.g., /autocomplete?q=sh)',
            '/health': 'Health check'
        }
    })

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'load':
        load_cities_to_redis()
    else:
        app.run(debug=True, host='0.0.0.0', port=5000)