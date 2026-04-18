import os
import sys
import urllib.request, json

from db_utils import handle_existing_data, string_to_bool, safe_int, safe_float


def run(conn, existed):

    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
    GOOGLE_BASE_URL = os.getenv('GOOGLE_BASE_URL')
    HEADERS = {"X-Goog_Api_Key": GOOGLE_API_KEY, "X-Goog-FieldMask": '*'}

    if not GOOGLE_API_KEY or not GOOGLE_BASE_URL:
        print("Error: Missing required environment variables for Google API. Please set GOOGLE_API_KEY and GOOGLE_BASE_URL in the .env file.")
        return

    run_state_park_places = True
    run_state_forests_places = True
    run_state_waysides_places = True
    run_national_forests_places = True

    if existed:
        run_state_park_places = handle_existing_data(conn, "google.places", "Google Places - State Parks", schema_name="google")
        run_state_forests_places = handle_existing_data(conn, "google.places", "Google Places - State Forests", schema_name="google")
        run_state_waysides_places = handle_existing_data(conn, "google.places", "Google Places - State Waysides", schema_name="google")
        run_national_forests_places = handle_existing_data(conn, "google.places", "Google Places - National Forests", schema_name="google")
        #TODO: truncate commands for each of the above if user says yes

    if not any([run_state_park_places, run_state_forests_places, run_state_waysides_places, run_national_forests_places]):
        print("Skipping Google Places job.")
        return

    print ("Running Google Places job.")

    conn.execute("CREATE SCHEMA IF NOT EXISTS google;")

    #TODO: Create Google Places tables if they don't exist, with appropriate columns and data types

    # TODO: Insert data into Google Places tables by making API calls to the Google Places API, using the GOOGLE_API_KEY and GOOGLE_BASE_URL, and parsing the responses to extract relevant information for state parks, state forests, state waysides, and national forests.

    # TODO: Handle pagination if the API returns paginated results, and ensure that all relevant data is retrieved and inserted into the database. Will be based on 