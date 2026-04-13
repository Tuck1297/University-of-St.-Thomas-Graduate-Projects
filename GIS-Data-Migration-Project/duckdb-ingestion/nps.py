import os
import requests
import duckdb
import urllib.request, json
def run(conn, existed):

    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    NPS_API_KEY = os.getenv('NPS_API_KEY')
    NPS_BASE_URL = os.getenv('NPS_BASE_URL')
    HEADERS = {"X-Api-Key": NPS_API_KEY}

    if not NPS_API_KEY or not NPS_BASE_URL:
        print("Error: NPS_API_KEY and NPS_BASE_URL must be set in the .env file.")
        return
    
    if existed:
        # TODO: If table already exists, decide if we want to drop and recreate it, or just skip the insertion.
        print("DuckDB database already exists. Skipping table creation and data insertion for NPS job.")
        return
    
    # Create the DuckDB tables for NPS data

    # TODO: Create all duckdb tables for NPS data

    # activities
    #     id: string
    #     name: string

    # amenities
    #     id: string
    #     name: string

    # campgrounds
    #     accessibility entity
    #     addresses entity
    #     amenities entity for campgrounds
    #     contacts entity
    #     campsites entity
    #     description: string
    #     directionsUrl: string
    #     fees entity
    #     id: number
    #     latitude: number
    #     longitude: number
    #     multimedia entity
    #     name: string
    #     parkCode: string
    #     regulationsUrl: string
    #     reservationUrl: string
    #     weatherOverview: string
    #     images entity
    #     operatingHours entity
    #     need to double check some of this based on querying the API

    # feespasses
    #     maybe wait with this one

    # parks
    #    id: string
    #    url: string
    #    fullName: string
    #    parkCode: string
    #    description: string
    #    latitude: number
    #    longitude: number
    #    activities entity for parks
    #    topics entity for parks
    #    states entity for parks
    #    contacts entity
    #    entrance fees entity
    #    entrance passes entity
    #    fees entity
    #    directionsInfo: string
    #    directionsUrl: string
    #    operatingHours entity
    #    addresses entity
    #    images entity
    #    weatherInfo: string
    #    name: string
    #    designation: string
    #    multimedia entity

    # retrieve activities
    endpoint = f"{NPS_BASE_URL}activities"

    # TODO: Retrieval and insertion logic here for

    # retrieve amenities
    endpoint = f"{NPS_BASE_URL}amenities"

    # TODO: Retrieval and insertion logic here for amenities

    # retrieve campgrounds
    endpoint = f"{NPS_BASE_URL}campgrounds"

    # TODO: Retrieval and insertion logic here for campgrounds

    # retrieve feespasses
    endpoint = f"{NPS_BASE_URL}feespasses"

    # TODO: Retrieval and insertion logic here for feespasses

    # retrieve parks
    endpoint = f"{NPS_BASE_URL}parks"

    req = urllib.request.Request(endpoint, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            parks = data.get('data', [])
            print(f"Fetched {len(parks)} parks from the NPS API.")

            # Insert into DuckDB

    except Exception as e:
        print(f"Error fetching parks data: {e}")
