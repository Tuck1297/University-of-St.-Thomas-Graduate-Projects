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

        truncate_commands = [
            "DELETE FROM google.places;",
            "DELETE FROM google.operating_hours;",
            "DELETE FROM google.photos;",
            "DELETE FROM google.address_descriptors;"
        ]

        run_state_park_places = handle_existing_data(conn, "google.places", "Google Places", truncate_commands=truncate_commands)

    if not any([run_state_park_places, run_state_forests_places, run_state_waysides_places, run_national_forests_places]):
        print("Skipping Google Places job.")
        return

    print ("Running Google Places job.")

    conn.execute("CREATE SCHEMA IF NOT EXISTS google;")

    # Google Place Entities

    conn.execute("""
        CREATE TABLE IF NOT EXISTS google.places (
            id INTEGER PRIMARY KEY,
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
            allowDogs BOOLEAN,
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
            nationalForest BOOLEAN,
            regularWeekdayDescriptions TEXT[],
            currentWeekdayDescriptions TEXT[]
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS google.operating_hours (
            id INTEGER PRIMARY KEY,
            placeId INTEGER,
            googlePlaceId TEXT,
            hoursType TEXT,
            dayOfWeek INTEGER,
            openHour INTEGER,
            openMinute INTEGER,
            closeHour INTEGER,
            closeMinute INTEGER,
            specificDate DATE
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS google.photos (
            id INTEGER PRIMARY KEY,
            placeId INTEGER,
            googlePlaceId TEXT,
            googlePhotoName TEXT,
            widthPx INTEGER,
            heightPx INTEGER,
            authorDisplayName TEXT,
            authorUri TEXT,
            photoUri TEXT
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS google.address_descriptors (
            id INTEGER PRIMARY KEY,
            placeId INTEGER,
            googlePlaceId TEXT,
            descriptorType TEXT,
            targetPlaceId TEXT,
            targetDisplayName TEXT,
            spatialRelationship TEXT,
            straightLineDistanceMeters FLOAT,
            travelDistanceMeters FLOAT,
            containment TEXT
        );
    """)

    def fetch_and_insert(query, park_flags):
        is_state_park, is_state_forest, is_state_wayside, is_national_forest = park_flags
        print(f"Fetch and insert Google Places data for: {query}")

        endpoint = f"{GOOGLE_BASE_URL}?key={GOOGLE_API_KEY}"
        next_page_token = None
        total_fetched = 0

        place_id = 1
        operating_hours_id = 1
        photos_id = 1
        address_descriptors_id = 1

        try:
            place_id = (conn.execute("SELECT MAX(id) FROM google.places").fetchone()[0] or 0) + 1
            operating_hours_id = (conn.execute("SELECT MAX(id) FROM google.operating_hours").fetchone()[0] or 0) + 1
            photos_id = (conn.execute("SELECT MAX(id) FROM google.photos").fetchone()[0] or 0) + 1
            address_descriptors_id = (conn.execute("SELECT MAX(id) FROM google.address_descriptors").fetchone()[0] or 0) + 1
        except Exception as e:
            print(f"Error fetching max IDs: {e}. Starting from 1.")


        while True:
            body = {
                "textQuery": query,
                "languageCode": "en",
                "regionCode": "US",
                "maxResultCount": 20
            }

            if next_page_token:
                body["pageToken"] = next_page_token

            req = urllib.request.Request(
                endpoint,
                headers={**HEADERS, "Content-Type": "application/json"},
                data=json.dumps(body).encode("utf-8"),
                method="POST"
            )

            current_table = "fetching data for Google Places"

            try:
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read())
                    places = data.get("places", [])
                    next_page_token = data.get("nextPageToken")

                    batch_count = len(places)
                    total_fetched += batch_count
                    print(f"Fetched {batch_count} places in this batch (Total: {total_fetched}) for '{query}'.")

                    for place in places:
                        googlePlaceId = place.get("id", "")
                        location = place.get("location", {})
                        displayName = place.get("displayName", {})
                        #primaryTypeDisplayName = place.get("primaryTypeDisplayName", {})
                        editorialSummary = place.get("editorialSummary", {})
                        paymentOptions = place.get("paymentOptions", {})
                        parkingOptions = place.get("parkingOptions", {})
                        accessibilityOptions = place.get("accessibilityOptions", {})
                        generativeSummary = place.get("generativeSummary", {})
                        postalAddress = place.get("postalAddress", {})
                        regularWeekdayDescriptions = place.get("regularOpeningHours", {}).get("weekdayDescriptions", [])
                        currentWeekdayDescriptions = place.get("currentOpeningHours", {}).get("weekdayDescriptions", [])

                        # Insert into google.places
                        try:
                            current_table = "google.places"
                            conn.execute("""
                                INSERT INTO google.places (
                                    id,
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
                                    addressLines,
                                    state,
                                    formattedAddress,
                                    shortFormattedAddress,
                                    statePark,
                                    stateForest,
                                    stateWayside,
                                    nationalForest,
                                    regularWeekdayDescriptions,
                                    currentWeekdayDescriptions
                                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                            """, (
                                place_id,
                                place.get("name", ""),
                                displayName.get("text", ""),
                                googlePlaceId,
                                place.get("nationalPhoneNumber", ""),
                                place.get("internationalPhoneNumber", ""),
                                safe_float(location.get("latitude")),
                                safe_float(location.get("longitude")),
                                place.get("websiteUri", ""),
                                place.get("googleMapsUri", ""),
                                place.get("businessStatus", ""),
                                place.get("primaryType", ""),
                                string_to_bool(place.get("goodForChildren", False)),
                                string_to_bool(place.get("allowsDogs", False)),
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
                                postalAddress.get("addressLines", []),
                                postalAddress.get("administrativeArea", ""),
                                place.get("formattedAddress", ""),
                                place.get("shortFormattedAddress", ""),
                                is_state_park, is_state_forest, is_state_wayside, is_national_forest,
                                regularWeekdayDescriptions,
                                currentWeekdayDescriptions
                            ))
                        except Exception as e:
                            print(f"Error inserting google.places ({googlePlaceId}): {e}")
                            continue

                        # Insert Operating Hours (Regular and Current)
                        for hours_type in ["regularOpeningHours", "currentOpeningHours"]:
                            hours_data = place.get(hours_type, {})
                            periods = hours_data.get("periods", [])
                            for period in periods:
                                open_period = period.get("open", {})
                                close_period = period.get("close", {})
                                current_table = "google.operating_hours"

                                specific_date = None
                                if hours_type == "currentOpeningHours" and "date" in open_period:
                                    d = open_period["date"]
                                    specific_date = f"{d.get('year')}-{d.get('month'):02d}-{d.get('day'):02d}"

                                conn.execute("""
                                    INSERT INTO google.operating_hours (
                                        id,
                                        placeId,
                                        googlePlaceId,
                                        hoursType,
                                        dayOfWeek,
                                        openHour,
                                        openMinute,
                                        closeHour,
                                        closeMinute,
                                        specificDate
                                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                                """, (
                                    operating_hours_id,
                                    place_id,
                                    googlePlaceId,
                                    "regular" if hours_type == "regularOpeningHours" else "current",
                                    safe_int(open_period.get("day")),
                                    safe_int(open_period.get("hour")),
                                    safe_int(open_period.get("minute")),
                                    safe_int(close_period.get("hour")),
                                    safe_int(close_period.get("minute")),
                                    specific_date
                                ))
                                operating_hours_id += 1

                        # Insert Photos
                        for photo in place.get("photos", []):
                            author_attribute = photo.get("authorAttributions", [{}])[0]
                            current_table = "google.photos"
                            conn.execute("""
                                INSERT INTO google.photos (
                                    id,
                                    placeId,
                                    googlePlaceId,
                                    googlePhotoName,
                                    widthPx,
                                    heightPx,
                                    authorDisplayName,
                                    authorUri,
                                    photoUri
                                ) VALUES (?,?,?,?,?,?,?,?,?)
                            """, (
                                photos_id,
                                place_id,
                                googlePlaceId,
                                photo.get("name", ""),
                                safe_int(photo.get("widthPx")),
                                safe_int(photo.get("heightPx")),
                                author_attribute.get("displayName", ""),
                                author_attribute.get("uri", ""),
                                photo.get("photoUri", "")
                            ))
                            photos_id += 1

                        # Insert Address Descriptors
                        address_desc = place.get("addressDescriptor", {})
                        for landmark in address_desc.get("landmarks", []):
                            current_table = "google.address_descriptors"
                            conn.execute("""
                                INSERT INTO google.address_descriptors (
                                    id,
                                    placeId,
                                    googlePlaceId,
                                    descriptorType,
                                    targetPlaceId,
                                    targetDisplayName,
                                    spatialRelationship,
                                    straightLineDistanceMeters,
                                    travelDistanceMeters
                                ) VALUES (?,?,?,?,?,?,?,?,?)
                            """, (
                                address_descriptors_id,
                                place_id,
                                googlePlaceId,
                                "landmark",
                                landmark.get("placeId"),
                                landmark.get("displayName", {}).get("text"),
                                landmark.get("spatialRelationship"),
                                safe_float(landmark.get("straightLineDistanceMeters")),
                                safe_float(landmark.get("travelDistanceMeters"))
                            ))
                            address_descriptors_id += 1

                        for area in address_desc.get("areas", []):
                            current_table = "google.address_descriptors"
                            conn.execute("""
                                INSERT INTO google.address_descriptors (
                                    id,
                                    placeId,
                                    googlePlaceId,
                                    descriptorType,
                                    targetPlaceId,
                                    targetDisplayName,
                                    containment
                                ) VALUES (?,?,?,?,?,?,?)
                            """, (
                                address_descriptors_id,
                                place_id,
                                googlePlaceId,
                                "area",
                                area.get("placeId"),
                                area.get("displayName", {}).get("text"),
                                area.get("containment")
                            ))
                            address_descriptors_id += 1

                        place_id += 1

                    if not next_page_token:
                        break

                    # Optional: small sleep to be safe with Google's token propagation
                    import time
                    time.sleep(1)

            except Exception as e:
                print(f"Error during {current_table} operation for places: {e}")
                break

    if run_state_park_places:
        fetch_and_insert("Minnesota state parks", (True, False, False, False))

    if run_state_forests_places:
        fetch_and_insert("Minnesota state forests", (False, True, False, False))

    if run_state_waysides_places:
        fetch_and_insert("Minnesota state waysides", (False, False, True, False))

    if run_national_forests_places:
        fetch_and_insert("National forests in Minnesota", (False, False, False, True))