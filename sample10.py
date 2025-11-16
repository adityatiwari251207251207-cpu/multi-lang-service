from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import sqlite3
import logging

# --- FILE-LEVEL INTERDEPENDENCY ---
# This API service imports the logic from the other file.
from analytics_processor import PostAnalyticsEngine, setup_database

# --- CONFIGURATION ---
DB_PATH = "social_analytics.db"
log = logging.getLogger(__name__)

# Initialize the Analytics Engine
# This creates the primary dependency. The API is useless without the engine.
try:
    engine = PostAnalyticsEngine(db_path=DB_PATH)
except Exception as e:
    log.critical(f"FATAL: Could not initialize Analytics Engine: {e}")
    # In a real app, you might exit or retry
    # For this example, we'll let it fail on requests
    engine = None 

# Initialize FastAPI App
app = FastAPI(
    title="Social Media Analytics API",
    description="API for processing and retrieving social media metrics."
)

# --- API MODELS ---
class Post(BaseModel):
    post_id: str
    user_id: str
    content: str
    platform: str

class Comment(BaseModel):
    comment_id: str
    post_id: str
    user_id: str
    comment_text: str

class Like(BaseModel):
    post_id: str
    user_id: str

class AnalysisResult(BaseModel):
    post_id: str
    engagement_rate: float
    sentiment_score: float
    trending_score: float

# --- API ENDPOINTS ---

@app.on_event("startup")
async def startup_event():
    """On startup, initialize the database schema."""
    # This endpoint's success is dependent on the setup_database function
    setup_database(DB_PATH)

def _get_db_conn():
    """Helper to get a direct DB connection for simple API tasks."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row # Return dicts instead of tuples
    return conn

# --- Data Ingestion Endpoints (Simple) ---
# These endpoints feed data into the system, which the analysis depends on.

@app.post("/ingest/post", status_code=201)
async def ingest_post(post: Post):
    """Ingests a new post into the database."""
    try:
        with _get_db_conn() as conn:
            conn.execute(
                "INSERT INTO posts (post_id, user_id, content, platform) VALUES (?, ?, ?, ?)",
                (post.post_id, post.user_id, post.content, post.platform)
            )
            conn.commit()
        return {"status": "success", "post_id": post.post_id}
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Post or user already exists or invalid user_id: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/comment", status_code=201)
async def ingest_comment(comment: Comment):
    """Ingests a new comment into the database."""
    try:
        with _get_db_conn() as conn:
            conn.execute(
                "INSERT INTO comments (comment_id, post_id, user_id, comment_text) VALUES (?, ?, ?, ?)",
                (comment.comment_id, comment.post_id, comment.user_id, comment.comment_text)
            )
            conn.commit()
        return {"status": "success", "comment_id": comment.comment_id}
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Comment exists or invalid post_id/user_id: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/like", status_code=201)
async def ingest_like(like: Like):
    """Ingests a new like into the database."""
    try:
        with _get_db_conn() as conn:
            conn.execute(
                "INSERT INTO likes (post_id, user_id) VALUES (?, ?)",
                (like.post_id, like.user_id)
            )
            conn.commit()
        return {"status": "success", "post_id": like.post_id, "user_id": like.user_id}
    except sqlite3.IntegrityError:
        # User already liked this post, not an error
        return {"status": "already_liked"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Analysis Endpoints (Complex & Interdependent) ---

@app.post("/analysis/run/{post_id}", response_model=AnalysisResult)
async def run_analysis(post_id: str, background_tasks: BackgroundTasks):
    """
    Triggers a full, complex analysis for a specific post.
    This demonstrates a background task dependency.
    """
    if not engine:
        raise HTTPException(status_code=503, detail="Analytics Engine is not running.")
    
    try:
        # **FUNCTIONAL DEPENDENCY**
        # This API endpoint calls the `run_full_analysis` orchestrator
        # from the 'analytics_processor.py' file.
        
        # We run this as a background task so the API returns immediately.
        # The 'engine.run_full_analysis' function itself contains all the
        # complex SQL and functional interdependencies.
        background_tasks.add_task(engine.run_full_analysis, post_id)
        
        return {"message": "Analysis job started in background.", "post_id": post_id}
    
    except Exception as e:
        log.error(f"Failed to start analysis job for {post_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to run analysis: {e}")

@app.get("/analysis/results/{post_id}", response_model=AnalysisResult)
async def get_analysis_results(post_id: str):
    """
    Retrieves the latest computed analysis results for a post.
    
    **SQL DEPENDENCY:**
    This endpoint's data is entirely dependent on the 'analytics_results'
    table being populated by the 'run_analysis' endpoint.
    """
    try:
        with _get_db_conn() as conn:
            result = conn.execute(
                "SELECT * FROM analytics_results WHERE post_id = ? ORDER BY calculation_timestamp DESC LIMIT 1",
                (post_id,)
            ).fetchone()
            
        if not result:
            raise HTTPException(status_code=404, detail="No analysis results found for this post. Run the analysis first.")
        
        # Convert the sqlite3.Row object (which is dict-like) to a dict
        return dict(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analysis/trending")
async def get_trending_posts():
    """
    Retrieves the top 10 trending posts.
    
    **SQL DEPENDENCY:**
    This query is dependent on the 'trending_score' calculated
    by the analytics engine and stored in 'analytics_results'.
    It also JOINS with the 'posts' table to get content.
    """
    query = """
    SELECT
        a.post_id,
        p.content,
        a.trending_score,
        a.calculation_timestamp
    FROM analytics_results AS a
    JOIN posts AS p ON a.post_id = p.post_id
    WHERE a.calculation_timestamp > ?
    ORDER BY a.trending_score DESC
    LIMIT 10;
    """
    time_window = (datetime.utcnow() - timedelta(hours=4)).isoformat()
    
    try:
        with _get_db_conn() as conn:
            results = conn.execute(query, (time_window,)).fetchall()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Main execution ---
if __name__ == "__main__":
    print("--- Starting Social Media Analytics API ---")
    print("--- Run `setup_database(DB_PATH)` if this is the first time ---")
    uvicorn.run(app, host="0.0.0.0", port=8000)