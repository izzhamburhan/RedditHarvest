from praw import Reddit
import praw
import sys

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

def extract_posts(reddit_instance: Reddit, subreddit:str, time_filter:str, limit:None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    
    posts_list = []

    print(posts)
    # for post in posts: