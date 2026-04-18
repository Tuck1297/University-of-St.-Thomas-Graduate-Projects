import os
import sys
import urllib.request, json

LIMIT = 1000

from db_utils import handle_existing_data, string_to_bool, safe_int, safe_float

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

    run_activities = True
    run_amenities = True
    run_topics = True
    run_campgrounds = True
    run_parks = True

    if existed:
        run_activities = handle_existing_data(conn, "nps.activities", "NPS Activities")
        run_amenities = handle_existing_data(conn, "nps.amenities", "NPS Amenities")
        run_topics = handle_existing_data(conn, "nps.topics", "NPS Topics")

        # Campground truncation commands
        truncate_campgrounds = [
            "DELETE FROM nps.campgrounds",
            "DELETE FROM nps.campground_amenities",
            "DELETE FROM nps.campground_campsites",
            "DELETE FROM nps.images WHERE campgroundId IS NOT NULL",
            "DELETE FROM nps.multimedia WHERE campgroundId IS NOT NULL",
            "DELETE FROM nps.operating_hours_exceptions WHERE operatingHoursId IN (SELECT id FROM nps.operating_hours WHERE campgroundId IS NOT NULL)",
            "DELETE FROM nps.operating_hours WHERE campgroundId IS NOT NULL",
            "DELETE FROM nps.fees WHERE campgroundId IS NOT NULL",
            "DELETE FROM nps.addresses WHERE campgroundId IS NOT NULL",
            "DELETE FROM nps.contact_phone_numbers WHERE campgroundId IS NOT NULL",
            "DELETE FROM nps.contact_email_addresses WHERE campgroundId IS NOT NULL",
        ]
        run_campgrounds = handle_existing_data(conn, "nps.campgrounds", "NPS Campgrounds", truncate_commands=truncate_campgrounds)

        # Park truncation commands
        truncate_parks = [
            "DELETE FROM nps.parks",
            "DELETE FROM nps.entrance_fees",
            "DELETE FROM nps.entrance_passes",
            "DELETE FROM nps.images WHERE parkId IS NOT NULL",
            "DELETE FROM nps.multimedia WHERE parkId IS NOT NULL",
            "DELETE FROM nps.operating_hours_exceptions WHERE operatingHoursId IN (SELECT id FROM nps.operating_hours WHERE parkId IS NOT NULL)",
            "DELETE FROM nps.operating_hours WHERE parkId IS NOT NULL",
            "DELETE FROM nps.fees WHERE parkId IS NOT NULL",
            "DELETE FROM nps.addresses WHERE parkId IS NOT NULL",
            "DELETE FROM nps.contact_phone_numbers WHERE parkId IS NOT NULL",
            "DELETE FROM nps.contact_email_addresses WHERE parkId IS NOT NULL",
        ]
        run_parks = handle_existing_data(conn, "nps.parks", "NPS Parks", truncate_commands=truncate_parks)

    if not any([run_activities, run_amenities, run_topics, run_campgrounds, run_parks]):
        print("All NPS sub-jobs skipped.")
        return

    print("Running NPS job")

    # Create the DuckDB tables for NPS data

    conn.execute("CREATE SCHEMA IF NOT EXISTS nps;")

    # activities

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.activities (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            name STRING
        );
    """)

    # topics

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.topics (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            name STRING
        );
    """)

    # amenities

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.amenities (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            name STRING
        );
    """)

    # campgrounds

    # Accessibility waiting with:
    #   classifications
    #   accessroads

    # Campground waiting with:
    #    regulationsoverview
    #    relevanceScore
    #
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.campgrounds (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            wheelChairAccess STRING,
            internetInfo STRING,
            rvAllowed BOOLEAN,
            cellPhoneInfo STRING,
            fireStovePolicy STRING,
            rvMaxLength INTEGER,
            additionalInfo STRING,
            trailMaxLength INTEGER,
            adaInfo STRING,
            rvInfo STRING,
            trailAllowed BOOLEAN,
            description STRING,
            directionsUrl STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            name STRING,
            parkCode STRING,
            regulationsUrl STRING,
            reservationsDescription STRING,
            reservationSitesFirstCome STRING,
            reservationsSitesReservable STRING,
            reservationsUrl STRING,
            weatherOverview STRING,
            accessRoads STRING[],
            classifications STRING[]
        );
    """)

    # Campground Amenities
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.campground_amenities (
            id INTEGER PRIMARY KEY,
            campgroundId INTEGER,
            trashRecyclingCollection STRING,
            toilets STRING[],
            internetConnectivity BOOLEAN,
            showers STRING[],
            cellPhoneReception BOOLEAN,
            laundry BOOLEAN,
            amphitheater STRING,
            dumpStation BOOLEAN,
            campStore BOOLEAN,
            staffOrVolunteerHostOnsite STRING,
            potableWater STRING[],
            iceAvailableForSale BOOLEAN,
            firewoodForSale BOOLEAN,
            foodStorageLockers STRING
        );
    """)

    # Campground Campsites
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.campground_campsites (
            id INTEGER PRIMARY KEY,
            campgroundId INTEGER,
            other INTEGER,
            "group" INTEGER,
            horse INTEGER,
            totalSites INTEGER,
            tentOnly INTEGER,
            rvOnly INTEGER,
            electricalHookups INTEGER,
            walkBoatTo INTEGER
        );
    """)

    # Images Entity for campgrounds and parks
    # Not tracking crops property
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.images (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            credit STRING,
            title STRING,
            altText STRING,
            caption STRING,
            url STRING
        );
    """)

    # Multimedia Entity for campgrounds and parks
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.multimedia (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            npsId STRING,
            title STRING,
            type STRING,
            url STRING
        );
    """)

    # Operating Hours entity for campgrounds and parks
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.operating_hours (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            description STRING,
            monday STRING,
            tuesday STRING,
            wednesday STRING,
            thursday STRING,
            friday STRING,
            saturday STRING,
            sunday STRING,
            name STRING
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.operating_hours_exceptions (
            id INTEGER PRIMARY KEY,
            operatingHoursId INTEGER,
            name STRING,
            startDate STRING,
            endDate STRING,
            monday STRING,
            tuesday STRING,
            wednesday STRING,
            thursday STRING,
            friday STRING,
            saturday STRING,
            sunday STRING
        );
    """)

    # Fees entity for campgrounds and parks
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.fees (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            cost DOUBLE,
            description STRING,
            title STRING
        );
    """)

    # Addresses entity for campgrounds and parks
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.addresses (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            postalCode STRING,
            city STRING,
            stateCode STRING,
            countryCode STRING,
            provinceTerritoryCode STRING,
            line1 STRING,
            line2 STRING,
            line3 STRING,
            type STRING
        );
    """)

    # Contacts entity for campgrounds and parks

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.contact_phone_numbers (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            description STRING,
            phoneNumber STRING,
            extension STRING,
            type STRING
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.contact_email_addresses (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            campgroundId INTEGER,
            emailAddress STRING,
            description STRING
        );
    """)

    # parks
    # fees array is empty for all parks data
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.parks (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            url STRING,
            fullName STRING,
            parkCode STRING,
            description STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            activities STRING[],
            topics STRING[],
            states STRING[],
            directionsInfo STRING,
            directionsUrl STRING,
            weatherInfo STRING,
            name STRING,
            designation STRING
        );
    """)

    # entrance fees
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.entrance_fees (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            cost DOUBLE,
            description STRING,
            title STRING
        );
    """)

    # entrance passes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nps.entrance_passes (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            cost DOUBLE,
            description STRING,
            title STRING
        );
    """)

    # retrieve activities
    if run_activities:
        print("Fetching activities data from NPS API...")
        endpoint = f"{NPS_BASE_URL}activities?limit=1000"
        req = urllib.request.Request(endpoint, headers=HEADERS)

        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                activities = data.get('data', [])
                print(f"Fetched {len(activities)} activities from the NPS API.")

                # Insert into DuckDB
                activity_id = 1
                for activity in activities:
                    try:
                        conn.execute("""
                            INSERT INTO nps.activities (id, npsId, name) VALUES (?, ?, ?);
                        """, (activity_id, activity.get('id'), activity.get('name')))
                        activity_id += 1
                    except Exception as e:
                        print(f"Error inserting into nps.activities: {e}")
                        sys.exit(1)

        except Exception as e:
            print(f"Error fetching activities data: {e}")
            sys.exit(1)

    # retrieve amenities
    if run_amenities:
        print("Fetching amenities data from NPS API...")
        endpoint = f"{NPS_BASE_URL}amenities?limit=1000"
        req = urllib.request.Request(endpoint, headers=HEADERS)

        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                amenities = data.get('data', [])
                print(f"Fetched {len(amenities)} amenities from the NPS API.")

                # Insert into DuckDB
                amenity_id = 1
                for amenity in amenities:
                    try:
                        conn.execute("""
                            INSERT INTO nps.amenities (id, npsId, name) VALUES (?, ?, ?);
                        """, (amenity_id, amenity.get('id'), amenity.get('name')))
                        amenity_id += 1
                    except Exception as e:
                        print(f"Error inserting into nps.amenities: {e}")
                        sys.exit(1)

        except Exception as e:
            print(f"Error fetching amenities data: {e}")
            sys.exit(1)

    # retrieve topics
    if run_topics:
        print("Fetching topics data from NPS API...")
        endpoint = f"{NPS_BASE_URL}topics?limit=1000"
        req = urllib.request.Request(endpoint, headers=HEADERS)
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                topics = data.get('data', [])
                print(f"Fetched {len(topics)} topics from the NPS API.")

                # Insert into DuckDB
                topic_id = 1
                for topic in topics:
                    try:
                        conn.execute("""
                            INSERT INTO nps.topics (id, npsId, name) VALUES (?, ?, ?);
                        """, (topic_id, topic.get('id'), topic.get('name')))
                        topic_id += 1
                    except Exception as e:
                        print(f"Error inserting into nps.topics: {e}")
                        sys.exit(1)
        except Exception as e:
            print(f"Error fetching topics data: {e}")
            sys.exit(1)

    # Build lookup mappings for activities and topics (npsId -> duckdb_id)
    activity_lookup = {}
    topic_lookup = {}

    try:
        activity_results = conn.execute("SELECT id, npsId FROM nps.activities").fetchall()
        for row in activity_results:
            activity_lookup[row[1]] = row[0]

        topic_results = conn.execute("SELECT id, npsId FROM nps.topics").fetchall()
        for row in topic_results:
            topic_lookup[row[1]] = row[0]

        print(f"Built lookup mappings: {len(activity_lookup)} activities, {len(topic_lookup)} topics")
    except Exception as e:
        print(f"Error building lookup mappings: {e}")
        sys.exit(1)

    # retrieve campgrounds
    if run_campgrounds:
        print("Fetching campgrounds data from NPS API...")
        endpoint = f"{NPS_BASE_URL}campgrounds?limit={LIMIT}"
        req = urllib.request.Request(endpoint, headers=HEADERS)

        # Initialize global ID counters
        campground_id = 1
        try:
            image_id = (conn.execute("SELECT MAX(id) FROM nps.images").fetchone()[0] or 0) + 1
            multimedia_id = (conn.execute("SELECT MAX(id) FROM nps.multimedia").fetchone()[0] or 0) + 1
            operating_hours_id = (conn.execute("SELECT MAX(id) FROM nps.operating_hours").fetchone()[0] or 0) + 1
            exception_id = (conn.execute("SELECT MAX(id) FROM nps.operating_hours_exceptions").fetchone()[0] or 0) + 1
            fees_id = (conn.execute("SELECT MAX(id) FROM nps.fees").fetchone()[0] or 0) + 1
            address_id = (conn.execute("SELECT MAX(id) FROM nps.addresses").fetchone()[0] or 0) + 1
            phone_number_id = (conn.execute("SELECT MAX(id) FROM nps.contact_phone_numbers").fetchone()[0] or 0) + 1
            email_id = (conn.execute("SELECT MAX(id) FROM nps.contact_email_addresses").fetchone()[0] or 0) + 1
        except:
            image_id = 1
            multimedia_id = 1
            operating_hours_id = 1
            exception_id = 1
            fees_id = 1
            address_id = 1
            phone_number_id = 1
            email_id = 1

        current_table = "fetching campgrounds data"
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                campgrounds = data.get('data', [])
                print(f"Fetched {len(campgrounds)} campgrounds from the NPS API.")

                for campground in campgrounds:
                    print("Inserting campground:", campground.get('name'))
                    # Insert main campground record
                    accessibility = campground.get('accessibility', {})
                    current_table = "nps.campgrounds"
                    conn.execute("""
                        INSERT INTO nps.campgrounds (
                            id,
                            npsId,
                            wheelChairAccess,
                            internetInfo,
                            rvAllowed,
                            cellPhoneInfo,
                            fireStovePolicy,
                            rvMaxLength,
                            additionalInfo,
                            trailMaxLength,
                            adaInfo,
                            rvInfo,
                            trailAllowed,
                            description,
                            directionsUrl,
                            latitude,
                            longitude,
                            name,
                            parkCode,
                            regulationsUrl,
                            reservationsDescription,
                            reservationSitesFirstCome,
                            reservationsSitesReservable,
                            reservationsUrl,
                            weatherOverview,
                            accessRoads,
                            classifications
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        campground_id,
                        campground.get('id'),
                        accessibility.get('wheelchairAccess', ''),
                        accessibility.get('internetInfo', ''),
                        string_to_bool(accessibility.get('rvAllowed', '0')),
                        accessibility.get('cellPhoneInfo', ''),
                        accessibility.get('fireStovePolicy', ''),
                        safe_int(accessibility.get('rvMaxLength')),
                        accessibility.get('additionalInfo', ''),
                        safe_int(accessibility.get('trailerMaxLength')),
                        accessibility.get('adaInfo', ''),
                        accessibility.get('rvInfo', ''),
                        string_to_bool(accessibility.get('trailerAllowed', '0')),
                        campground.get('description'),
                        campground.get('directionsUrl', ''),
                        safe_float(campground.get('latitude', 0)),
                        safe_float(campground.get('longitude', 0)),
                        campground.get('name'),
                        campground.get('parkCode'),
                        campground.get('regulationsurl', ''),
                        campground.get('reservationInfo', ''),
                        campground.get('numberOfSitesFirstComeFirstServe', 0),
                        campground.get('numberOfSitesReservable', 0),
                        campground.get('reservationUrl', ''),
                        campground.get('weatherOverview', ''),
                        campground.get('accessRoads', []),
                        campground.get('classifications', [])
                    ))

                    # Insert campground amenities
                    amenities_data = campground.get('amenities', {})
                    if amenities_data:
                        current_table = "nps.campground_amenities"
                        conn.execute("""
                            INSERT INTO nps.campground_amenities (
                                id, 
                                campgroundId, 
                                trashRecyclingCollection, 
                                toilets,
                                internetConnectivity, 
                                showers, 
                                cellPhoneReception,
                                laundry, 
                                amphitheater, 
                                dumpStation, 
                                campStore,
                                staffOrVolunteerHostOnsite, 
                                potableWater,
                                iceAvailableForSale, 
                                firewoodForSale, 
                                foodStorageLockers
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            campground_id,
                            campground_id,
                            amenities_data.get('trashRecyclingCollection', ''),
                            amenities_data.get('toilets', []),
                            string_to_bool(amenities_data.get('internetConnectivity', 'No')),
                            amenities_data.get('showers', []),
                            string_to_bool(amenities_data.get('cellPhoneReception', 'No')),
                            string_to_bool(amenities_data.get('laundry', 'No')),
                            amenities_data.get('amphitheater', ''),
                            string_to_bool(amenities_data.get('dumpStation', 'No')),
                            string_to_bool(amenities_data.get('campStore', 'No')),
                            amenities_data.get('staffOrVolunteerHostOnsite', ''),
                            amenities_data.get('potableWater', []),
                            string_to_bool(amenities_data.get('iceAvailableForSale', 'No')),
                            string_to_bool(amenities_data.get('firewoodForSale', 'No')),
                            amenities_data.get('foodStorageLockers', '')
                        ))

                    # Insert campground campsites
                    campsites = campground.get('campsites', {})
                    if campsites:
                        current_table = "nps.campground_campsites"
                        conn.execute("""
                            INSERT INTO nps.campground_campsites (
                                id, 
                                campgroundId, 
                                totalSites, 
                                "group", 
                                horse,
                                tentOnly, 
                                rvOnly, 
                                electricalHookups,
                                walkBoatTo, 
                                other
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            campground_id,
                            campground_id,
                            safe_int(campsites.get('totalSites')),
                            safe_int(campsites.get('group')),
                            safe_int(campsites.get('horse')),
                            safe_int(campsites.get('tentOnly')),
                            safe_int(campsites.get('rvOnly')),
                            safe_int(campsites.get('electricalHookups')),
                            safe_int(campsites.get('walkBoatTo')),
                            safe_int(campsites.get('other'))
                        ))

                    # Insert contact phone numbers
                    contacts = campground.get('contacts', {})
                    phone_numbers = contacts.get('phoneNumbers', [])

                    for phone in phone_numbers:
                        current_table = "nps.contact_phone_numbers"
                        conn.execute("""
                            INSERT INTO nps.contact_phone_numbers (
                                id,
                                parkId, 
                                campgroundId, 
                                description, 
                                phoneNumber, 
                                extension,
                                type
                            ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (
                            phone_number_id,
                            None,
                            campground_id,
                            phone.get('description', ''),
                            phone.get('phoneNumber', ''),
                            phone.get('extension', ''),
                            phone.get('type', '')
                        ))
                        phone_number_id += 1

                    # Insert contact email addresses
                    email_addresses = contacts.get('emailAddresses', [])
                    for email in email_addresses:
                        current_table = "nps.contact_email_addresses"
                        conn.execute("""
                            INSERT INTO nps.contact_email_addresses (
                                id,
                                parkId, 
                                campgroundId, 
                                emailAddress, 
                                description
                            ) VALUES (?, ?, ?, ?, ?);
                        """, (
                            email_id,
                            None,
                            campground_id,
                            email.get('emailAddress', ''),
                            email.get('description', '')
                        ))
                        email_id += 1

                    # Insert fees
                    fees_list = campground.get('fees', [])

                    for fee in fees_list:
                        current_table = "nps.fees"
                        conn.execute("""
                            INSERT INTO nps.fees (
                                id,
                                parkId, 
                                campgroundId, 
                                cost, 
                                description, 
                                title
                            ) VALUES (?, ?, ?, ?, ?, ?);
                        """, (
                            fees_id,
                            None,
                            campground_id,
                            safe_float(fee.get('cost')),
                            fee.get('description', ''),
                            fee.get('title', '')
                        ))
                        fees_id += 1

                    # Insert operating hours
                    operating_hours_list = campground.get('operatingHours', [])

                    for op_hours in operating_hours_list:
                        standard_hours = op_hours.get('standardHours', {})
                        current_table = "nps.operating_hours"
                        conn.execute("""
                            INSERT INTO nps.operating_hours (
                                id, 
                                parkId,
                                campgroundId, 
                            description, 
                            monday, 
                            tuesday,
                            wednesday, 
                            thursday, 
                            friday, 
                            saturday, 
                            sunday, 
                            name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        operating_hours_id,
                        None,
                        campground_id,
                        op_hours.get('description', ''),
                        standard_hours.get('monday', ''),
                        standard_hours.get('tuesday', ''),
                        standard_hours.get('wednesday', ''),
                        standard_hours.get('thursday', ''),
                        standard_hours.get('friday', ''),
                        standard_hours.get('saturday', ''),
                        standard_hours.get('sunday', ''),
                        op_hours.get('name', '')
                    ))
                    
                        # Insert exceptions for this operating hours entry
                        exceptions = op_hours.get('exceptions', [])
                        for exception in exceptions:
                            exception_hours = exception.get('exceptionHours', {})
                            current_table = "nps.operating_hours_exceptions"
                            conn.execute("""
                                INSERT INTO nps.operating_hours_exceptions (
                                    id,
                                    operatingHoursId, 
                                    name, 
                                    startDate, 
                                    endDate,
                                    monday, 
                                    tuesday, 
                                    wednesday, 
                                    thursday,
                                    friday, 
                                    saturday, 
                                    sunday
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                            """, (
                                exception_id,
                                operating_hours_id,
                                exception.get('name', ''),
                                exception.get('startDate', ''),
                                exception.get('endDate', ''),
                                exception_hours.get('monday', ''),
                                exception_hours.get('tuesday', ''),
                                exception_hours.get('wednesday', ''),
                                exception_hours.get('thursday', ''),
                                exception_hours.get('friday', ''),
                                exception_hours.get('saturday', ''),
                                exception_hours.get('sunday', '')
                            ))
                            exception_id += 1
                        
                        operating_hours_id += 1

                    # Insert images
                    images_list = campground.get('images', [])
                    for image in images_list:
                        current_table = "nps.images"
                        conn.execute("""
                            INSERT INTO nps.images (
                                id,
                                parkId,
                                campgroundId, 
                                credit, 
                                title, 
                                altText, 
                                caption, 
                                url
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            image_id,
                            None,
                            campground_id,
                            image.get('credit', ''),
                            image.get('title', ''),
                            image.get('altText', ''),
                            image.get('caption', ''),
                            image.get('url', '')
                        ))
                        image_id += 1

                    # Insert multimedia
                    multimedia_list = campground.get('multimedia', [])
                    for media in multimedia_list:
                        current_table = "nps.multimedia"
                        conn.execute("""
                            INSERT INTO nps.multimedia (
                                id,
                                parkId,
                                campgroundId, 
                                npsId, 
                                title, 
                                type, 
                                url
                            ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (
                            multimedia_id,
                            None,
                            campground_id,
                            campground.get('id'),
                            media.get('title', ''),
                            media.get('type', ''),
                            media.get('url', '')
                        ))
                        multimedia_id += 1

                    # Insert Addresses
                    addresses_list = campground.get('addresses', [])
                    for address in addresses_list:
                        current_table = "nps.addresses"
                        conn.execute("""
                            INSERT INTO nps.addresses (
                                id,
                                parkId,
                                campgroundId,
                                postalCode,
                                city,
                                stateCode,
                                countryCode,
                                provinceTerritoryCode,
                                line1,
                                line2,
                                line3,
                                type
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            address_id,
                            None,
                            campground_id,
                            address.get('postalCode', ''),
                            address.get('city', ''),
                            address.get('stateCode', ''),
                            address.get('countryCode', ''),
                            address.get('provinceTerritoryCode', ''),
                            address.get('line1', ''),
                            address.get('line2', ''),
                            address.get('line3', ''),
                            address.get('type', '')
                        ))
                        address_id += 1

                    campground_id += 1

        except Exception as e:
            print(f"Error during {current_table} operation for campgrounds: {e}")
            sys.exit(1)

    # retrieve parks
    if run_parks:
        print("Fetching parks data from NPS API...")
        endpoint = f"{NPS_BASE_URL}parks?limit={LIMIT}"
        req = urllib.request.Request(endpoint, headers=HEADERS)

        # Initialize global ID counters (starting high enough to avoid collision if run independently)
        # However, for a clean truncate/refetch, we can reset them if we ensure consistency
        park_id = 1
        # Re-initialize shared counters to avoid collision with existing campground data if it wasn't truncated
        # Or better: query the current max ID. For simplicity here, we assume they start from 1 if we're refetching.
        # But if we ONLY refetched parks, we might collide.
        # Fixed logic: Get max IDs from DB
        try:
            image_id = (conn.execute("SELECT MAX(id) FROM nps.images").fetchone()[0] or 0) + 1
            multimedia_id = (conn.execute("SELECT MAX(id) FROM nps.multimedia").fetchone()[0] or 0) + 1
            operating_hours_id = (conn.execute("SELECT MAX(id) FROM nps.operating_hours").fetchone()[0] or 0) + 1
            exception_id = (conn.execute("SELECT MAX(id) FROM nps.operating_hours_exceptions").fetchone()[0] or 0) + 1
            fees_id = (conn.execute("SELECT MAX(id) FROM nps.fees").fetchone()[0] or 0) + 1
            address_id = (conn.execute("SELECT MAX(id) FROM nps.addresses").fetchone()[0] or 0) + 1
            phone_number_id = (conn.execute("SELECT MAX(id) FROM nps.contact_phone_numbers").fetchone()[0] or 0) + 1
            email_id = (conn.execute("SELECT MAX(id) FROM nps.contact_email_addresses").fetchone()[0] or 0) + 1
            entrance_fee_id = 1
            entrance_pass_id = 1
        except:
            image_id = 1
            multimedia_id = 1
            operating_hours_id = 1
            exception_id = 1
            fees_id = 1
            address_id = 1
            phone_number_id = 1
            email_id = 1
            entrance_fee_id = 1
            entrance_pass_id = 1

        current_table = "fetching parks data"
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                parks = data.get('data', [])
                print(f"Fetched {len(parks)} parks from the NPS API.")

                for park in parks:
                    print("Inserting park:", park.get('fullName'))
                    # Insert main park record
                    states = park.get('states', '')
                    states_list = [s.strip() for s in states.split(',')] if states else []
                    activities = park.get('activities', [])
                    topics = park.get('topics', [])

                    # Map NPS IDs to DuckDB IDs for activities and topics
                    activity_ids = [activity_lookup.get(act.get('id')) for act in activities if activity_lookup.get(act.get('id'))]
                    topic_ids = [topic_lookup.get(top.get('id')) for top in topics if topic_lookup.get(top.get('id'))]

                    current_table = "nps.parks"
                    conn.execute("""
                        INSERT INTO nps.parks (
                            id, 
                            npsId, 
                            url, 
                            fullName, 
                            parkCode, 
                            description,
                            latitude, 
                            longitude, 
                            activities,
                            topics,
                            states,
                            directionsInfo, 
                            directionsUrl,
                            weatherInfo,
                            name,
                            designation 
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        park_id,
                        park.get('id'),
                        park.get('url', ''),
                        park.get('fullName', ''),
                        park.get('parkCode', ''),
                        park.get('description', ''),
                        safe_float(park.get('latitude', 0)),
                        safe_float(park.get('longitude', 0)),
                        activity_ids,
                        topic_ids,
                        states_list,
                        park.get('directionsInfo', ''),
                        park.get('directionsUrl', ''),
                        park.get('weatherInfo', ''),
                        park.get('name', ''),
                        park.get('designation', '')
                    ))

                    # Insert contact phone numbers
                    contacts = park.get('contacts', {})
                    phone_numbers = contacts.get('phoneNumbers', [])
                    for phone in phone_numbers:
                        current_table = "nps.contact_phone_numbers"
                        conn.execute("""
                            INSERT INTO nps.contact_phone_numbers (
                                id, parkId, campgroundId, phoneNumber, description, type, extension
                            ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (
                            phone_number_id,
                            park_id,
                            None,
                            phone.get('phoneNumber', ''),
                            phone.get('description', ''),
                            phone.get('type', ''),
                            phone.get('extension', '')
                        ))
                        phone_number_id += 1

                    # Insert contact email addresses
                    email_addresses = contacts.get('emailAddresses', [])
                    for email in email_addresses:
                        current_table = "nps.contact_email_addresses"
                        conn.execute("""
                            INSERT INTO nps.contact_email_addresses (
                                id, parkId, campgroundId, emailAddress, description
                            ) VALUES (?, ?, ?, ?, ?);
                        """, (
                            email_id,
                            park_id,
                            None,
                            email.get('emailAddress', ''),
                            email.get('description', '')
                        ))
                        email_id += 1

                    # Insert entrance fees
                    entrance_fees = park.get('entranceFees', [])
                    for fee in entrance_fees:
                        current_table = "nps.entrance_fees"
                        conn.execute("""
                            INSERT INTO nps.entrance_fees (
                                id,
                                parkId, 
                                cost, 
                                description, 
                                title
                            ) VALUES (?, ?, ?, ?, ?);
                        """, (
                            entrance_fee_id,
                            park_id,
                            safe_float(fee.get('cost')),
                            fee.get('description', ''),
                            fee.get('title', '')
                        ))
                        entrance_fee_id += 1

                    # Insert entrance passes
                    entrance_passes = park.get('entrancePasses', [])
                    for pass_item in entrance_passes:
                        current_table = "nps.entrance_passes"
                        conn.execute("""
                            INSERT INTO nps.entrance_passes (
                                id,
                                parkId, 
                                cost, 
                                description, 
                                title
                            ) VALUES (?, ?, ?, ?, ?);
                        """, (
                            entrance_pass_id,
                            park_id,
                            safe_float(pass_item.get('cost')),
                            pass_item.get('description', ''),
                            pass_item.get('title', '')
                        ))
                        entrance_pass_id += 1

                    # Insert operating hours
                    park_operating_hours_list = park.get('operatingHours', [])
                    for op_hours in park_operating_hours_list:
                        standard_hours = op_hours.get('standardHours', {})
                        current_table = "nps.operating_hours"
                        conn.execute("""
                            INSERT INTO nps.operating_hours (
                                id, 
                                parkId,
                                campgroundId, 
                                description, 
                                monday, 
                                tuesday,
                                wednesday, 
                                thursday, 
                                friday, 
                                saturday, 
                                sunday, 
                                name
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            operating_hours_id,
                            park_id,
                            None,
                            op_hours.get('description', ''),
                            standard_hours.get('monday', ''),
                            standard_hours.get('tuesday', ''),
                            standard_hours.get('wednesday', ''),
                            standard_hours.get('thursday', ''),
                            standard_hours.get('friday', ''),
                            standard_hours.get('saturday', ''),
                            standard_hours.get('sunday', ''),
                            op_hours.get('name', '')
                        ))

                        # Insert exceptions for this operating hours entry
                        exceptions = op_hours.get('exceptions', [])
                        for exception in exceptions:
                            exception_hours = exception.get('exceptionHours', {})
                            current_table = "nps.operating_hours_exceptions"
                            conn.execute("""
                                INSERT INTO nps.operating_hours_exceptions (
                                    id,
                                    operatingHoursId, 
                                    name, 
                                    startDate, 
                                    endDate,
                                    monday, 
                                    tuesday, 
                                    wednesday, 
                                    thursday,
                                    friday, 
                                    saturday, 
                                    sunday
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                            """, (
                                exception_id,
                                operating_hours_id,
                                exception.get('name', ''),
                                exception.get('startDate', ''),
                                exception.get('endDate', ''),
                                exception_hours.get('monday', ''),
                                exception_hours.get('tuesday', ''),
                                exception_hours.get('wednesday', ''),
                                exception_hours.get('thursday', ''),
                                exception_hours.get('friday', ''),
                                exception_hours.get('saturday', ''),
                                exception_hours.get('sunday', '')
                            ))
                            exception_id += 1
                        
                        operating_hours_id += 1

                    # Insert images
                    park_images = park.get('images', [])
                    for image in park_images:
                        current_table = "nps.images"
                        conn.execute("""
                            INSERT INTO nps.images (
                                id,
                                parkId, 
                                campgroundId,
                                credit, 
                                title, 
                                altText, 
                                caption, 
                                url
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            image_id,
                            park_id,
                            None,
                            image.get('credit', ''),
                            image.get('title', ''),
                            image.get('altText', ''),
                            image.get('caption', ''),
                            image.get('url', '')
                        ))
                        image_id += 1

                    # Insert multimedia
                    park_multimedia = park.get('multimedia', [])
                    for media in park_multimedia:
                        current_table = "nps.multimedia"
                        conn.execute("""
                            INSERT INTO nps.multimedia (
                                id, parkId, campgroundId, npsId, title, type, url
                            ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (
                            multimedia_id,
                            park_id,
                            None,
                            park.get('id'),
                            media.get('title', ''),
                            media.get('type', ''),
                            media.get('url', '')
                        ))
                        multimedia_id += 1


                    # Insert Addresses
                    park_addresses = park.get('addresses', [])
                    for address in park_addresses:
                        current_table = "nps.addresses"
                        conn.execute("""
                            INSERT INTO nps.addresses (
                                id,
                                parkId,
                                campgroundId,
                                postalCode,
                                city,
                                stateCode,
                                countryCode,
                                provinceTerritoryCode,
                                line1,
                                line2,
                                line3,
                                type
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
                            address_id,
                            park_id,
                            None,
                            address.get('postalCode', ''),
                            address.get('city', ''),
                            address.get('stateCode', ''),
                            address.get('countryCode', ''),
                            address.get('provinceTerritoryCode', ''),
                            address.get('line1', ''),
                            address.get('line2', ''),
                            address.get('line3', ''),
                            address.get('type', '')
                        ))
                        address_id += 1

                    park_id += 1

        except Exception as e:
            print(f"Error during {current_table} operation for parks: {e}")
            sys.exit(1)
    
    # Commit the transaction to ensure data is persisted to disk
    conn.commit()
    print("NPS job completed successfully.")