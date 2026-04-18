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

    # Google Place

    conn.execute("""
        CREATE TABLE IF NOT EXISTS google.places (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            googleName TEXT,
            primaryDisplayName TEXT,
            googlePlaceId TEXT,
            nationalPhoneNumber TEXT,
            internationalPhoneNumber TEXT,
            latitude FLOAT,
            longitude FLOAT,
            websiteUri TEXT,
            googleMapsUri TEXT,
            businessStatus TEXT,
            primaryType TEXT,
            goodForChildren BOOLEAN,
            allowDogs: BOOLEAN,
            acceptCreditCards BOOLEAN,
            acceptDebitCards BOOLEAN,
            acceptCashOnly BOOLEAN,
            freeParkingLot BOOLEAN,
            paidParkingLot BOOLEAN,
            freeStreetParking BOOLEAN,
            wheelchairAccessibleParking BOOLEAN,
            wheelchairAccessibleSeating BOOLEAN,
            geminiGenerativeSummary TEXT,
            editorialSummary TEXT,
            regionCode TEXT,
            languageCode TEXT,
            postalCode TEXT,
            administrativeArea TEXT,
            locality TEXT,
            addressLines TEXT[],
            state TEXT,
            formattedAddress TEXT,
            shortFormattedAddress TEXT,
            statePark BOOLEAN,
            stateForest BOOLEAN,
            stateWayside BOOLEAN,
            nationalForest BOOLEAN
        );
    """)

    # TODO: regularOperatingHours Entity
    # TODO: currentOpeningHours Entity
    # TODO: photos Entity
    # TODO: addressDescriptor Entity


    if run_state_park_places:
        print("Fetch and insert Google Places data for State Parks")
        endpoint = f"{GOOGLE_BASE_URL}?key={GOOGLE_API_KEY}"
        body = {
            "textQuery": "Minnesota state parks",
            "languageCode": "en",
            "regionCode": "US",
            "maxResultCount": 60
        }

        req = urllib.request.Request(
            endpoint,
            headers={**HEADERS, "Content-Type": "application/json"},
            data=json.dumps(body).encode("utf-8"),
            method="POST"
        )

        try:

            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                places = data.get("places", [])
                print(f"Fetched {len(places)} places for Minnesota state parks from Google Places API.")

                # Insert itno DuckDB

                for place in places:
                    location = place.get("location", {})
                    regularOperatingHours = place.get("regularOperatingHours", {})
                    displayName = place.get("displayName", {})
                    primaryTypeDisplayName = displayName.get("primaryDisplayName", {})
                    currentOpeningHours = place.get("currentOpeningHours", {})
                    editorialSummary = place.get("editorialSummary", {})
                    photos = place.get("photos", [])
                    paymentOptions = place.get("paymentOptions", {})
                    parkingOptions = place.get("parkingOptions", {})
                    accessibilityOptions = place.get("accessibilityOptions", {})
                    generativeSummary = place.get("generativeSummary", {})
                    addressDescriptor = place.get("addressDescriptor", {})
                    postalAddress = place.get("postalAddress", {})

                    try:
                        conn.execute("""
                            INSERT INTO google.places (
                                googleName,
                                primaryDisplayName,
                                googlePlaceId,
                                nationalPhoneNumber,
                                internationalPhoneNumber,
                                latitude,
                                longitude,
                                websiteUri,
                                googleMapsUri,
                                businessStatus,
                                primaryType,
                                goodForChildren,
                                allowDogs,
                                acceptCreditCards,
                                acceptDebitCards,
                                acceptCashOnly,
                                freeParkingLot,
                                paidParkingLot,
                                freeStreetParking,
                                wheelchairAccessibleParking,
                                wheelchairAccessibleSeating,
                                geminiGenerativeSummary,
                                editorialSummary,
                                regionCode,
                                languageCode,
                                postalCode,
                                administrativeArea,
                                locality,
                                addressLines[],
                                state,
                                formattedAddress,
                                shortFormattedAddress,
                                statePark,
                                stateForest,
                                stateWayside,
                                nationalForest
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                        """, (
                            place.get("name", ""),
                            primaryTypeDisplayName.get("text", ""),
                            place.get("id", ""),
                            place.get("nationalPhoneNumber", ""),
                            place.get("internationalPhoneNumber", ""),
                            location.get("latitude", ""),
                            location.get("longitude", ""),
                            place.get("websiteUri", ""),
                            place.get("googleMapsUri", ""),
                            place.get("businessStatus", ""),
                            place.get("primaryType", ""),
                            string_to_bool(place.get("goodForChildren", False)),
                            string_to_bool(place.get("allowDogs", False)),
                            string_to_bool(paymentOptions.get("acceptCreditCards", False)),
                            string_to_bool(paymentOptions.get("acceptDebitCards", False)),
                            string_to_bool(paymentOptions.get("acceptCashOnly", False)),
                            string_to_bool(parkingOptions.get("freeParkingLot", False)),
                            string_to_bool(parkingOptions.get("paidParkingLot", False)),
                            string_to_bool(parkingOptions.get("freeStreetParking", False)),
                            string_to_bool(accessibilityOptions.get("wheelchairAccessibleParking", False)),
                            string_to_bool(accessibilityOptions.get("wheelchairAccessibleSeating", False)),
                            generativeSummary.get("overview", {}).get("text", ""),
                            editorialSummary.get("text", ""),
                            postalAddress.get("regionCode", ""),
                            postalAddress.get("languageCode", ""),
                            postalAddress.get("postalCode", ""),
                            postalAddress.get("administrativeArea", ""),
                            postalAddress.get("locality", ""),
                            json.dumps(postalAddress.get("addressLines", [])),
                            postalAddress.get("administrativeArea", ""),
                            postalAddress.get("formattedAddress", ""),
                            place.get("shortFormattedAddress", ""),
                            True,  # statePark
                            False, # stateForest
                            False, # stateWayside
                            False  # nationalForest
                        ))
                    except Exception as e:
                        print(f"Error inserting google.places into database: {e}")
                        sys.exit(1)


        except Exception as e:
            print(f"Error fetching Google Places data for State Parks: {e}")
            return


    # TODO: Insert data into Google Places tables by making API calls to the Google Places API, using the GOOGLE_API_KEY and GOOGLE_BASE_URL, and parsing the responses to extract relevant information for state parks, state forests, state waysides, and national forests.

    # TODO: Handle pagination if the API returns paginated results, and ensure that all relevant data is retrieved and inserted into the database. Will be based on 