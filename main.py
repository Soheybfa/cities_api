from flask import Flask, request, jsonify
import redis
import json
import sys

app = Flask(__name__)

# Redis connection with connection pooling for performance
redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=50
)

# Redis key prefixes
CITY_PREFIX = "city:"
NAME_INDEX_PREFIX = "name:"
SEARCH_PREFIX = "search:"

def load_cities_to_redis(json_file='cities.json'):
    """Load cities from JSON into Redis with multiple indexes"""
    print("Loading cities into Redis...")
    
    with open(json_file, 'r') as f:
        cities = json.load(f)
    
    pipe = redis_client.pipeline()
    
    for city in cities:
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
            'total_keys': redis_client.dbsize()
        })
    except:
        return jsonify({'status': 'error', 'redis': 'disconnected'}), 503

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
        app.run(debug=True, port=5000)