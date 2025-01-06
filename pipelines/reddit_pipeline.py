from etls.reddit_etl import connect_reddit, extract_posts, load_data_to_csv, transform_data
from utils.constants import CLIENT_ID, OUTPUT_PATH, SECRET
import pandas as pd

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    #connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    
    #extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    
    #transformation
    post_df  = transform_data(post_df)

    #loading to csv
    load_data_to_csv(post_df, OUTPUT_PATH)