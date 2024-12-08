from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import os
from datetime import datetime
import logging
import sys

# Import our existing data processor
from defi_processor import DeFiChainDataProcessor

app = FastAPI(title="DeFi Chain Fees API")

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup Redis connection
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.from_url(REDIS_URL)
CACHE_KEY = "defi_chain_data"
CACHE_EXPIRATION = 60 * 60 * 24  # 24 hours in seconds

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Startup event
@app.on_event("startup")
async def startup_event():
    logger.info("Starting FastAPI application")
    try:
        # Force a data refresh on startup
        await get_fresh_data()
        logger.info("Initial data fetch completed")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")

async def get_fresh_data():
    """Fetch fresh data from DefiLlama API"""
    try:
        logger.info("Fetching fresh data from DefiLlama...")
        processor = DeFiChainDataProcessor()
        dates, chain_data = processor.get_time_series_format()
        
        data = {
            "dates": dates,
            "chainData": chain_data,
            "lastUpdated": datetime.now().isoformat()
        }
        
        # Store in Redis with 24-hour expiration
        redis_client.setex(
            CACHE_KEY,
            CACHE_EXPIRATION,
            json.dumps(data)
        )
        
        logger.info("Successfully cached fresh data")
        return data
        
    except Exception as e:
        logger.error(f"Error fetching fresh data: {str(e)}")
        raise

async def get_cached_or_fresh_data():
    """Get data from cache if available, otherwise fetch fresh"""
    try:
        # Try to get from cache first
        cached_data = redis_client.get(CACHE_KEY)
        
        if cached_data:
            logger.info("Returning cached data")
            return json.loads(cached_data)
        
        # If no cached data, get fresh data
        return await get_fresh_data()
        
    except redis.RedisError as e:
        logger.error(f"Redis error: {str(e)}")
        # If Redis fails, fallback to fresh data
        return await get_fresh_data()

@app.get("/api/chain-fees")
async def get_chain_fees(chain_filter: str = None):
    """Get chain fees data, optionally filtered by chain"""
    try:
        data = await get_cached_or_fresh_data()
        
        if chain_filter:
            if chain_filter not in data["chainData"]:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Chain {chain_filter} not found"
                )
            return {
                "dates": data["dates"],
                "chainData": {chain_filter: data["chainData"][chain_filter]},
                "lastUpdated": data["lastUpdated"]
            }
        
        return data
        
    except Exception as e:
        logger.error(f"Error in get_chain_fees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """Health check endpoint that also verifies Redis connection"""
    logger.info("Health check called")
    try:
        redis_client.ping()
        logger.info("Redis connection successful")
        return {
            "status": "healthy",
            "redis": "connected"
        }
    except redis.RedisError:
        logger.error(f"Redis connection failed: {str(e)}")
        return {
            "status": "healthy",
            "redis": "disconnected"
        }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
