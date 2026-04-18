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

        # Minnesota Grid: Divided into 4 quadrants to bypass the 60-result limit
        # Approximate bounds: Lat 43.5 to 49.4, Lon -97.2 to -89.5
        quadrants = [
            {"name": "Southwest", "low": {"latitude": 43.49, "longitude": -97.24}, "high": {"latitude": 46.5, "longitude": -94.5}},
            {"name": "Southeast", "low": {"latitude": 43.49, "longitude": -94.5}, "high": {"latitude": 46.5, "longitude": -89.49}},
            {"name": "Northwest", "low": {"latitude": 46.5, "longitude": -97.24}, "high": {"latitude": 49.38, "longitude": -94.5}},
            {"name": "Northeast", "low": {"latitude": 46.5, "longitude": -94.5}, "high": {"latitude": 49.38, "longitude": -89.49}},
        ]

        endpoint = f"{GOOGLE_BASE_URL}?key={GOOGLE_API_KEY}"
        total_fetched = 0
        
        # Track seen IDs across quadrants to avoid duplicates
        seen_google_ids = set()
        
        # Fetch existing IDs from DB to avoid duplicates if resuming (though we usually truncate)
        try:
            existing = conn.execute("SELECT googlePlaceId FROM google.places").fetchall()
            seen_google_ids.update([r[0] for r in existing])
        except:
            pass

        # Global counters (fetched at start of job in run() or fetch_and_insert())
        # We need to ensure these are passed or accessible. In this script they are local.
        # Let's pull them inside the function scope to be safe.
        try:
            p_id = (conn.execute("SELECT MAX(id) FROM google.places").fetchone()[0] or 0) + 1
            oh_id = (conn.execute("SELECT MAX(id) FROM google.operating_hours").fetchone()[0] or 0) + 1
            ph_id = (conn.execute("SELECT MAX(id) FROM google.photos").fetchone()[0] or 0) + 1
            ad_id = (conn.execute("SELECT MAX(id) FROM google.address_descriptors").fetchone()[0] or 0) + 1
        except:
            p_id, oh_id, ph_id, ad_id = 1, 1, 1, 1

        for quad in quadrants:
            print(f"  Searching quadrant: {quad['name']}...")
            next_page_token = None
            
            while True:
                body = {
                    "textQuery": query,
                    "languageCode": "en",
                    "regionCode": "US",
                    "maxResultCount": 20,
                    "locationRestriction": {
                        "rectangle": {
                            "low": quad["low"],
                            "high": quad["high"]
                        }
                    }
                }

                if next_page_token:
                    body["pageToken"] = next_page_token

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
                        next_page_token = data.get("nextPageToken")

                        batch_count = 0
                        for place in places:
                            googlePlaceId = place.get("id", "")
                            
                            # Deduplication check
                            if googlePlaceId in seen_google_ids:
                                continue
                            
                            seen_google_ids.add(googlePlaceId)
                            batch_count += 1
                            total_fetched += 1
                            
                            location = place.get("location", {})
                            displayName = place.get("displayName", {})
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
                                conn.execute("""
                                    INSERT INTO google.places (
                                        id, googleName, primaryDisplayName, googlePlaceId, nationalPhoneNumber,
                                        internationalPhoneNumber, latitude, longitude, websiteUri,
                                        googleMapsUri, businessStatus, primaryType, goodForChildren,
                                        allowDogs, acceptCreditCards, acceptDebitCards, acceptCashOnly,
                                        freeParkingLot, paidParkingLot, freeStreetParking,
                                        wheelchairAccessibleParking, wheelchairAccessibleSeating,
                                        geminiGenerativeSummary, editorialSummary, regionCode,
                                        languageCode, postalCode, administrativeArea, locality,
                                        addressLines, state, formattedAddress, shortFormattedAddress,
                                        statePark, stateForest, stateWayside, nationalForest,
                                        regularWeekdayDescriptions, currentWeekdayDescriptions
                                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                                """, (
                                    p_id, place.get("name", ""), displayName.get("text", ""), googlePlaceId,
                                    place.get("nationalPhoneNumber", ""), place.get("internationalPhoneNumber", ""),
                                    safe_float(location.get("latitude")), safe_float(location.get("longitude")),
                                    place.get("websiteUri", ""), place.get("googleMapsUri", ""), place.get("businessStatus", ""),
                                    place.get("primaryType", ""), string_to_bool(place.get("goodForChildren", False)),
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
                                    postalAddress.get("regionCode", ""), postalAddress.get("languageCode", ""),
                                    postalAddress.get("postalCode", ""), postalAddress.get("administrativeArea", ""),
                                    postalAddress.get("locality", ""), postalAddress.get("addressLines", []),
                                    postalAddress.get("administrativeArea", ""), place.get("formattedAddress", ""),
                                    place.get("shortFormattedAddress", ""),
                                    is_state_park, is_state_forest, is_state_wayside, is_national_forest,
                                    regularWeekdayDescriptions, currentWeekdayDescriptions
                                ))
                            except Exception as e:
                                print(f"    Error inserting google.places ({googlePlaceId}): {e}")
                                continue

                            # Insert Operating Hours
                            for hours_type in ["regularOpeningHours", "currentOpeningHours"]:
                                hours_data = place.get(hours_type, {})
                                periods = hours_data.get("periods", [])
                                for period in periods:
                                    open_period = period.get("open", {})
                                    close_period = period.get("close", {})
                                    specific_date = None
                                    if hours_type == "currentOpeningHours" and "date" in open_period:
                                        d = open_period["date"]
                                        specific_date = f"{d.get('year')}-{d.get('month'):02d}-{d.get('day'):02d}"

                                    conn.execute("""
                                        INSERT INTO google.operating_hours (
                                            id, placeId, googlePlaceId, hoursType, dayOfWeek,
                                            openHour, openMinute, closeHour, closeMinute, specificDate
                                        ) VALUES (?,?,?,?,?,?,?,?,?,?)
                                    """, (
                                        oh_id, p_id, googlePlaceId,
                                        "regular" if hours_type == "regularOpeningHours" else "current",
                                        safe_int(open_period.get("day")), safe_int(open_period.get("hour")),
                                        safe_int(open_period.get("minute")), safe_int(close_period.get("hour")),
                                        safe_int(close_period.get("minute")), specific_date
                                    ))
                                    oh_id += 1

                            # Insert Photos
                            for photo in place.get("photos", []):
                                attr = photo.get("authorAttributions", [{}])[0]
                                conn.execute("""
                                    INSERT INTO google.photos (
                                        id, placeId, googlePlaceId, googlePhotoName, widthPx, heightPx,
                                        authorDisplayName, authorUri, photoUri
                                    ) VALUES (?,?,?,?,?,?,?,?,?)
                                """, (
                                    ph_id, p_id, googlePlaceId, photo.get("name", ""),
                                    safe_int(photo.get("widthPx")), safe_int(photo.get("heightPx")),
                                    attr.get("displayName", ""), attr.get("uri", ""), photo.get("photoUri", "")
                                ))
                                ph_id += 1

                            # Insert Address Descriptors
                            address_desc = place.get("addressDescriptor", {})
                            for landmark in address_desc.get("landmarks", []):
                                conn.execute("""
                                    INSERT INTO google.address_descriptors (
                                        id, placeId, googlePlaceId, descriptorType, targetPlaceId,
                                        targetDisplayName, spatialRelationship, straightLineDistanceMeters,
                                        travelDistanceMeters
                                    ) VALUES (?,?,?,?,?,?,?,?,?)
                                """, (
                                    ad_id, p_id, googlePlaceId, "landmark", landmark.get("placeId"),
                                    landmark.get("displayName", {}).get("text"), landmark.get("spatialRelationship"),
                                    safe_float(landmark.get("straightLineDistanceMeters")),
                                    safe_float(landmark.get("travelDistanceMeters"))
                                ))
                                ad_id += 1
                            for area in address_desc.get("areas", []):
                                conn.execute("""
                                    INSERT INTO google.address_descriptors (
                                        id, placeId, googlePlaceId, descriptorType, targetPlaceId,
                                        targetDisplayName, containment
                                    ) VALUES (?,?,?,?,?,?,?)
                                """, (
                                    ad_id, p_id, googlePlaceId, "area", area.get("placeId"),
                                    area.get("displayName", {}).get("text"), area.get("containment")
                                ))
                                ad_id += 1

                            p_id += 1
                        
                        print(f"    Fetched {batch_count} new places in this batch (Quadrant Total: {total_fetched}).")

                        if not next_page_token:
                            break
                        import time
                        time.sleep(1)

                except Exception as e:
                    print(f"  Error fetching data for quadrant {quad['name']}: {e}")
                    break

    if run_state_park_places:
        fetch_and_insert("Minnesota state parks", (True, False, False, False))

    if run_state_forests_places:
        fetch_and_insert("Minnesota state forests", (False, True, False, False))

    if run_state_waysides_places:
        fetch_and_insert("Minnesota state waysides", (False, False, True, False))

    if run_national_forests_places:
        fetch_and_insert("National forests in Minnesota", (False, False, False, True))