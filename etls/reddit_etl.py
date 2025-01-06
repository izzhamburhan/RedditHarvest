import numpy as np
from praw import Reddit
import praw
import sys
import pandas as pd

from utils.constants import POST_FIELDS

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id=client_id,  
            client_secret=client_secret, 
            user_agent=user_agent
            )
        print("Successfully connected to Reddit!")
        return reddit
    except Exception as e:
        print(f"Error connecting to Reddit: {e}")
        return None
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit: None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    
    posts_list = []
    
    for post in posts:
        post_data = {field: getattr(post, field) for field in POST_FIELDS}
        # Special handling for author since it needs to be converted to string
        post_data['author'] = str(post_data['author'])
        posts_list.append(post_data)
    
    return posts_list


def transform_data(post_df: pd.DataFrame):
    # Validate dataframe has all required columns
    missing_cols = set(POST_FIELDS) - set(post_df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = post_df['over_18'].astype(bool)
    post_df['author'] = post_df['author'].astype(str)
    
    # Ensure columns are in correct order
    post_df = post_df[list(POST_FIELDS)]
    
    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
    print(f"Data loaded successfully to {path}")