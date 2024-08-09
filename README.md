# Dependencies
import requests
import time
from dotenv import load_dotenv
import os
import pandas as pd
import json

# Set environment variables from the .env in the local environment
load_dotenv()
nyt_api_key = os.getenv("NYT_API_KEY")
tmdb_api_key = os.getenv("TMDB_API_KEY")

# Set the base URL
url = "https://api.nytimes.com/svc/search/v2/articlesearch.json?"

# Filter for movie reviews with "love" in the headline
# section_name should be "Movies"
# type_of_material should be "Review"
filter_query = 'section_name:"Movies" AND type_of_material:"Review" AND headline:"love"'

# Use a sort filter, sort by newest
sort = "newest"

# Select the following fields to return:
# headline, web_url, snippet, source, keywords, pub_date, byline, word_count
field_list = "headline,web_url,snippet,source,keywords,pub_date,byline,word_count"

# Search for reviews published between a begin and end date
begin_date = "20130101"
end_date = "20230531"

# Build URL
url_query = (f"{url}api-key={nyt_api_key}&begin_date={begin_date}&end_date={end_date}"
    + f'&fq={filter_query}&sort={sort}&fl={field_list}')

# Check status code
url_query = (f"{url}api-key={nyt_api_key}&begin_date={begin_date}&end_date={end_date}"
    + f'&fq={filter_query}&sort={sort}&fl={field_list}')

r = requests.get(url_query)
print(f"Status code: {r.status_code}")

# Create an empty list to store the reviews
reviews_list = []

# loop through pages 0-19
for page in range (20):
    # create query with a page number
    # API results show 10 articles at a time
    query_url = f"{url_query}&page={page}"
    
    # Make a "GET" request and retrieve the JSON
    articles= requests.get(query_url).json()
    
    # Add a twelve second interval between queries to stay within API query limits
    time.sleep(12)
    
    # Try and save the reviews to the reviews_list
    try: 
        # loop through the reviews["response"]["docs"] and append each review to the list
        for review in articles['response']['docs']:
            reviews_list.append(review)
        # Print the page that was just retrieved
        print(f"Checked page {page}")

        # Print the page number that had no results then break from the loop
    except requests.exceptions.RequestException as e:
        print(f"Error occurred on page {page}: {e}")
        break

# Preview the first 5 results in JSON format
# Use json.dumps with argument indent=4 to format data
print(json.dumps(reviews_list[:5], indent=4))

# Convert reviews_list to a Pandas DataFrame using json_normalize()
results_list_df = pd.json_normalize(reviews_list)
results_list_df

# Extract the title from the "headline.main" column and
# save it to a new column "title"
# Title is between unicode characters \u2018 and \u2019. 
# End string should include " Review" to avoid cutting title early
cols = list(results_list_df.columns)
cols[7] = 'headline.main'
results_list_df.columns 

results_list_df['title'] = results_list_df['headline.main'].apply(lambda st: st[st.find("\u2018")+1:st.find("\u2019 Review")])
results_list_df

# Function to extract 'name' and 'value' from items in "keywords" column
def extract_keywords(keyword_list):
    if isinstance(keyword_list, list):
        extracted_keywords = ""
        for item in keyword_list:
            # Ensure item is a dictionary with 'name' and 'value' keys
            if isinstance(item, dict) and 'name' in item and 'value' in item:
                keyword = f"{item['name']}: {item['value']}; "
                extracted_keywords += keyword
        return extracted_keywords.strip()
    else:
        return str(keyword_list)  # If not a list, return as string

# Fix the "keywords" column by converting cells from a list to a string
results_list_df['keywords'] = results_list_df['keywords'].apply(extract_keywords)
results_list_df

# Create a list from the "title" column using to_list()
# These titles will be used in the query for The Movie Database
titles = results_list_df['title'].to_list()
titles

# Prepare The Movie Database query
base_url = "https://api.themoviedb.org/3/search/movie?query="
tmdb_key_string = "&api_key=" + tmdb_api_key

# Create an empty list to store the results
tmdb_movies_list = []

# Create a request counter to sleep the requests after a multiple
# of 50 requests
request_counter = 0
max_request = 200

# Loop through the titles
for title in titles:
    
    # Check if we need to sleep before making a request
    if request_counter % max_request == 0 and request_counter != 0:
        print("Sleeping for 1 second to avoid hitting the rate limit...")
        time.sleep(1)  
        
    # Add 1 to the request counter
    request_counter += 1
    
    # Perform a "GET" request for The Movie Database
    query_url = base_url + title + tmdb_key_string
    response = requests.get(query_url).json()
  
    # Include a try clause to search for the full movie details.
    # Use the except clause to print out a statement if a movie
    # is not found.
    try:
        if not response['results']:
            print(f"{title} not found")
            continue
        # Get movie id
        movie_id = response['results'][0]['id']

        # Make a request for a the full movie details
        movie_details_query = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={tmdb_api_key}"

        # Execute "GET" request with url
        response_details = requests.get(movie_details_query).json()
        
        # Extract the genre names into a list
        genre_names = [genre['name'] for genre in response_details.get('genres', [])]

        # Extract the spoken_languages' English name into a list
        spoken_languages = [lang['english_name'] for lang in response_details.get('spoken_languages', [])]

        # Extract the production_countries' name into a list
        production_countries = [country['name'] for country in response_details.get('production_countries', [])]

        # Add the relevant data to a dictionary
        title = response_details.get('title', 'N/A')
        original_title = response_details.get('original_title', 'N/A')  # Extract original title
        budget = response_details.get('budget', 0)
        language = response_details.get('original_language', 'N/A')
        homepage = response_details.get('homepage', 'N/A')
        overview = response_details.get('overview', 'N/A')
        popularity = response_details.get('popularity', 0.0)
        runtime = response_details.get('runtime', 0)
        revenue = response_details.get('revenue', 0)
        release_date = response_details.get('release_date', 'N/A')
        vote_average = response_details.get('vote_average', 0.0)
        vote_count = response_details.get('vote_count', 0)
            
        # Append data to the tmdb_movies_list list
        movie_info = {
            'title': title,
            'original_title' : original_title,
            'budget' : budget,
            'genres': genre_names,
            'language' : language,
            'spoken_languages': spoken_languages,
            'hompage' : homepage,
            'overview' : overview,
            'popularity' : popularity,
            'runtime' : runtime,
            'revenue' : revenue,
            'release_date' : release_date,
            'vote_average' : vote_average,
            'vote_count' : vote_count,
            'production_countries': production_countries,
        }
        tmdb_movies_list.append(movie_info)
        # Print out the title that was found
        print(f"Found {movie_info['title']}")
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while retrieving data for title: {title} - {e}")

# Preview the first 5 results in JSON format
# Use json.dumps with argument indent=4 to format data
first_five_tmdb_movies = tmdb_movies_list[:5]
formatted_tmdb_json = json.dumps(first_five_tmdb_movies, indent=4)
print(formatted_tmdb_json)

# Convert the results to a DataFrame
movies_df = pd.DataFrame(tmdb_movies_list)
movies_df

# Merge the New York Times reviews and TMDB DataFrames on title
merged_df = pd.merge(movies_df, results_list_df, on='title', how='inner')
merged_df

# Remove list brackets and quotation marks on the columns containing lists
# Create a list of the columns that need fixing
columns_to_fix = ['genres', 'spoken_languages', 'production_countries']

# Create a list of characters to remove
characters_to_remove = ["[", "]", "'", "\""]

# Loop through the list of columns to fix
for col in columns_to_fix:
    # Convert the column to type 'str'
    merged_df[col] = merged_df[col].astype(str)

    # Loop through characters to remove
    for char in characters_to_remove:
        merged_df[col] = merged_df[col].str.replace(char, '')

# Display the fixed DataFrame
merged_df.head()

# Drop "byline.person" column
if 'byline.person' in merged_df.columns:
    merged_df.drop(columns=['byline.person'], inplace=True)

# Delete duplicate rows and reset index
merged_df.drop_duplicates(inplace=True)
merged_df.reset_index(drop=True, inplace=True)
merged_df.head()

# Export data to CSV without the index
merged_df.to_csv('merged_movie_reviews.csv', index=False)