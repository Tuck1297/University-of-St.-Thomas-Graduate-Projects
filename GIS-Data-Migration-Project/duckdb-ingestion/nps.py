import os
import sys
import requests
import duckdb
import urllib.request, json

def string_to_bool(value):
    """Convert string '1'/'0' or empty to boolean. Returns False for empty or '0', True for '1'."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip() == '1'
    return bool(value) if value else False

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

    print("Running NPS job")

    # Create the DuckDB tables for NPS data

    # activities

    conn.execute("""
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            name STRING
        );
    """)

    # topics

    conn.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY,
            npsId STRING,
            name STRING
        );
    """)

    # amenities

    conn.execute("""
        CREATE TABLE IF NOT EXISTS amenities (
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
        CREATE TABLE IF NOT EXISTS campgrounds (
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
        CREATE TABLE IF NOT EXISTS campground_amenities (
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
        CREATE TABLE IF NOT EXISTS campground_campsites (
            id INTEGER PRIMARY KEY,
            campgroundId INTEGER,
            other INTEGER,
            group INTEGER,
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
        CREATE TABLE IF NOT EXISTS images (
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
        CREATE TABLE IF NOT EXISTS multimedia (
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
        CREATE TABLE IF NOT EXISTS operating_hours (
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
        CREATE TABLE IF NOT EXISTS operating_hours_exceptions (
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
        CREATE TABLE IF NOT EXISTS fees (
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
        CREATE TABLE IF NOT EXISTS addresses (
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
        CREATE TABLE IF NOT EXISTS contact_phone_numbers (
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
        CREATE TABLE IF NOT EXISTS contact_email_addresses (
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
        CREATE TABLE IF NOT EXISTS parks (
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
        CREATE TABLE IF NOT EXISTS entrance_fees (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            cost DOUBLE,
            description STRING,
            title STRING
        );
    """)

    # entrance passes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entrance_passes (
            id INTEGER PRIMARY KEY,
            parkId INTEGER,
            cost DOUBLE,
            description STRING,
            title STRING
        );
    """)

    # retrieve activities
    endpoint = f"{NPS_BASE_URL}activities?limit=1000"
    req = urllib.request.Request(endpoint, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            activities = data.get('data', [])
            print(f"Fetched {len(activities)} activities from the NPS API.")

            # Insert into DuckDB
            for activity in activities:
                conn.execute("""
                    INSERT INTO activities (npsId, name) VALUES (?, ?);
                """, (activity.get('id'), activity.get('name')))

    except Exception as e:
        print(f"Error fetching activities data: {e}")
        sys.exit(1)

    # retrieve amenities
    endpoint = f"{NPS_BASE_URL}amenities?limit=1000"
    req = urllib.request.Request(endpoint, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            amenities = data.get('data', [])
            print(f"Fetched {len(amenities)} amenities from the NPS API.")

            # Insert into DuckDB
            for amenity in amenities:
                conn.execute("""
                    INSERT INTO amenities (npsId, name) VALUES (?, ?);
                """, (amenity.get('id'), amenity.get('name')))

    except Exception as e:
        print(f"Error fetching amenities data: {e}")
        sys.exit(1)

    # retrieve topics
    endpoint = f"{NPS_BASE_URL}topics?limit=1000"
    req = urllib.request.Request(endpoint, headers=HEADERS)
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            topics = data.get('data', [])
            print(f"Fetched {len(topics)} topics from the NPS API.")

            # Insert into DuckDB
            for topic in topics:
                conn.execute("""
                    INSERT INTO topics (npsId, name) VALUES (?, ?);
                """, (topic.get('id'), topic.get('name')))
    except Exception as e:
        print(f"Error fetching topics data: {e}")
        sys.exit(1)

    # TODO: Review from here...

    # retrieve campgrounds
    endpoint = f"{NPS_BASE_URL}campgrounds?limit=1000"
    req = urllib.request.Request(endpoint, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            campgrounds = data.get('data', [])
            print(f"Fetched {len(campgrounds)} campgrounds from the NPS API.")

            campground_id = 1
            image_id = 1
            operating_hours_id = 1

            for campground in campgrounds:
                # Insert main campground record
                accessibility = campground.get('accessibility', {})
                conn.execute("""
                    INSERT INTO campgrounds (
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
                    int(accessibility.get('rvMaxLength', 0)) if accessibility.get('rvMaxLength') else 0,
                    accessibility.get('additionalInfo', ''),
                    int(accessibility.get('trailerMaxLength', 0)) if accessibility.get('trailerMaxLength') else 0,
                    accessibility.get('adaInfo', ''),
                    accessibility.get('rvInfo', ''),
                    string_to_bool(accessibility.get('trailerAllowed', '0')),
                    campground.get('description'),
                    campground.get('directionsUrl', ''),
                    float(campground.get('latitude', 0)),
                    float(campground.get('longitude', 0)),
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
                # TODO: combine amenities into main campground entity
                amenities = campground.get('amenities', {})
                if amenities:
                    conn.execute("""
                        INSERT INTO campground_amenities (
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
                        amenities.get('trashRecyclingCollection', ''),
                        amenities.get('toilets', []),
                        amenities.get('internetConnectivity', ''),
                        amenities.get('showers', []),
                        amenities.get('cellPhoneReception', ''),
                        amenities.get('laundry', '0'),
                        amenities.get('amphitheater', ''),
                        amenities.get('dumpStation', ''),
                        amenities.get('campStore', ''),
                        amenities.get('staffOrVolunteerHostOnsite', ''),
                        amenities.get('potableWater', []),
                        amenities.get('iceAvailableForSale', ''),
                        amenities.get('firewoodForSale', ''),
                        amenities.get('foodStorageLockers', '')
                    ))

                # Insert campground campsites
                # TODO: combine campsites into main campground entity
                campsites = campground.get('campsites', {})
                if campsites:
                    conn.execute("""
                        INSERT INTO campground_campsites (
                            id, 
                            campgroundId, 
                            totalSites, 
                            group, 
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
                        int(campsites.get('totalSites', 0)) if campsites.get('totalSites') else 0,
                        int(campsites.get('group', 0)) if campsites.get('group') else 0,
                        int(campsites.get('horse', 0)) if campsites.get('horse') else 0,
                        int(campsites.get('tentOnly', 0)) if campsites.get('tentOnly') else 0,
                        int(campsites.get('rvOnly', 0)) if campsites.get('rvOnly') else 0,
                        int(campsites.get('electricalHookups', 0)) if campsites.get('electricalHookups') else 0,
                        int(campsites.get('walkBoatTo', 0)) if campsites.get('walkBoatTo') else 0,
                        int(campsites.get('other', 0)) if campsites.get('other') else 0
                    ))

                # TODO: Pick Up here later...

                # Insert contact phone numbers
                contacts = campground.get('contacts', {})
                phone_numbers = contacts.get('phoneNumbers', [])
                for phone in phone_numbers:
                    conn.execute("""
                        INSERT INTO contact_phone_numbers (
                            parkId, campgroundId, phoneNumber, description, type
                        ) VALUES (?, ?, ?, ?, ?);
                    """, (
                        None,
                        campground_id,
                        phone.get('phoneNumber', ''),
                        phone.get('description', ''),
                        phone.get('type', '')
                    ))

                # Insert contact email addresses
                email_addresses = contacts.get('emailAddresses', [])
                for email in email_addresses:
                    conn.execute("""
                        INSERT INTO contact_email_addresses (
                            parkId, campgroundId, emailAddress, description
                        ) VALUES (?, ?, ?, ?);
                    """, (
                        None,
                        campground_id,
                        email.get('emailAddress', ''),
                        email.get('description', '')
                    ))

                # Insert fees
                fees = campground.get('fees', [])
                for fee in fees:
                    conn.execute("""
                        INSERT INTO fees (
                            parkId, campgroundId, cost, description, title
                        ) VALUES (?, ?, ?, ?, ?);
                    """, (
                        None,
                        campground_id,
                        float(fee.get('cost', 0)),
                        fee.get('description', ''),
                        fee.get('title', '')
                    ))

                # Insert operating hours
                operating_hours_list = campground.get('operatingHours', [])
                operating_hours_mapping = {}
                for op_hours in operating_hours_list:
                    standard_hours = op_hours.get('standardHours', {})
                    conn.execute("""
                        INSERT INTO operating_hours (
                            id, campgroundId, description, monday, tuesday,
                            wednesday, thursday, friday, saturday, sunday, name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        operating_hours_id,
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
                    operating_hours_mapping[operating_hours_id] = op_hours
                    
                    # Insert exceptions for this operating hours entry
                    exceptions = op_hours.get('exceptions', [])
                    for exception in exceptions:
                        exception_hours = exception.get('exceptionHours', {})
                        conn.execute("""
                            INSERT INTO operating_hours_exceptions (
                                operatingHoursId, name, startDate, endDate,
                                monday, tuesday, wednesday, thursday,
                                friday, saturday, sunday
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
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
                    
                    operating_hours_id += 1

                # Insert images
                images = campground.get('images', [])
                for image in images:
                    conn.execute("""
                        INSERT INTO images (
                            campgroundId, credit, title, altText, caption, url
                        ) VALUES (?, ?, ?, ?, ?, ?);
                    """, (
                        campground_id,
                        image.get('credit', ''),
                        image.get('title', ''),
                        image.get('altText', ''),
                        image.get('caption', ''),
                        image.get('url', '')
                    ))

                # Insert multimedia
                multimedia = campground.get('multimedia', [])
                for media in multimedia:
                    conn.execute("""
                        INSERT INTO multimedia (
                            campgroundId, npsId, title, type, url
                        ) VALUES (?, ?, ?, ?, ?);
                    """, (
                        campground_id,
                        campground.get('id'),
                        media.get('title', ''),
                        media.get('type', ''),
                        media.get('url', '')
                    ))

                campground_id += 1

    except Exception as e:
        print(f"Error fetching campgrounds data: {e}")
        sys.exit(1)

    # retrieve parks
    endpoint = f"{NPS_BASE_URL}parks?limit=1000"
    req = urllib.request.Request(endpoint, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            parks = data.get('data', [])
            print(f"Fetched {len(parks)} parks from the NPS API.")

            park_id = 1
            image_id = 1
            operating_hours_id = 1

            for park in parks:
                # Insert main park record
                states = park.get('states', '')
                if isinstance(states, list):
                    states = ','.join(states)

                conn.execute("""
                    INSERT INTO parks (
                        id, npsId, url, fullName, parkCode, description,
                        latitude, longitude, directionsInfo, directionsUrl, states
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    park_id,
                    park.get('id'),
                    park.get('url', ''),
                    park.get('fullName', ''),
                    park.get('parkCode', ''),
                    park.get('description', ''),
                    float(park.get('latitude', 0)),
                    float(park.get('longitude', 0)),
                    park.get('directionsInfo', ''),
                    park.get('directionsUrl', ''),
                    states
                ))

                # Insert park activities
                activities = park.get('activities', [])
                for activity in activities:
                    conn.execute("""
                        INSERT INTO activities (npsId, name) VALUES (?, ?);
                    """, (activity.get('id'), activity.get('name')))

                # Insert park topics
                topics = park.get('topics', [])
                for topic in topics:
                    conn.execute("""
                        INSERT INTO topics (npsId, name) VALUES (?, ?);
                    """, (topic.get('id'), topic.get('name')))

                # Insert contact phone numbers
                contacts = park.get('contacts', {})
                phone_numbers = contacts.get('phoneNumbers', [])
                for phone in phone_numbers:
                    conn.execute("""
                        INSERT INTO contact_phone_numbers (
                            parkId, campgroundId, phoneNumber, description, type
                        ) VALUES (?, ?, ?, ?, ?);
                    """, (
                        park_id,
                        None,
                        phone.get('phoneNumber', ''),
                        phone.get('description', ''),
                        phone.get('type', '')
                    ))

                # Insert contact email addresses
                email_addresses = contacts.get('emailAddresses', [])
                for email in email_addresses:
                    conn.execute("""
                        INSERT INTO contact_email_addresses (
                            parkId, campgroundId, emailAddress, description
                        ) VALUES (?, ?, ?, ?);
                    """, (
                        park_id,
                        None,
                        email.get('emailAddress', ''),
                        email.get('description', '')
                    ))

                # Insert entrance fees
                entrance_fees = park.get('entranceFees', [])
                for fee in entrance_fees:
                    conn.execute("""
                        INSERT INTO entrance_fees (
                            parkId, cost, description, title
                        ) VALUES (?, ?, ?, ?);
                    """, (
                        park_id,
                        float(fee.get('cost', 0)),
                        fee.get('description', ''),
                        fee.get('title', '')
                    ))

                # Insert entrance passes
                entrance_passes = park.get('entrancePasses', [])
                for pass_item in entrance_passes:
                    conn.execute("""
                        INSERT INTO entrance_passes (
                            parkId, cost, description, title
                        ) VALUES (?, ?, ?, ?);
                    """, (
                        park_id,
                        float(pass_item.get('cost', 0)),
                        pass_item.get('description', ''),
                        pass_item.get('title', '')
                    ))

                # Insert operating hours
                operating_hours_list = park.get('operatingHours', [])
                for op_hours in operating_hours_list:
                    standard_hours = op_hours.get('standardHours', {})
                    conn.execute("""
                        INSERT INTO operating_hours (
                            id, parkId, description, monday, tuesday,
                            wednesday, thursday, friday, saturday, sunday, name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        operating_hours_id,
                        park_id,
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
                        conn.execute("""
                            INSERT INTO operating_hours_exceptions (
                                operatingHoursId, name, startDate, endDate,
                                monday, tuesday, wednesday, thursday,
                                friday, saturday, sunday
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (
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
                    
                    operating_hours_id += 1

                # Insert images
                images = park.get('images', [])
                for image in images:
                    conn.execute("""
                        INSERT INTO images (
                            parkId, credit, title, altText, caption, url
                        ) VALUES (?, ?, ?, ?, ?, ?);
                    """, (
                        park_id,
                        image.get('credit', ''),
                        image.get('title', ''),
                        image.get('altText', ''),
                        image.get('caption', ''),
                        image.get('url', '')
                    ))

                # Insert multimedia
                multimedia = park.get('multimedia', [])
                for media in multimedia:
                    conn.execute("""
                        INSERT INTO multimedia (
                            parkId, npsId, title, type, url
                        ) VALUES (?, ?, ?, ?, ?);
                    """, (
                        park_id,
                        park.get('id'),
                        media.get('title', ''),
                        media.get('type', ''),
                        media.get('url', '')
                    ))

                park_id += 1

    except Exception as e:
        print(f"Error fetching parks data: {e}")
        sys.exit(1)
    
    # Commit the transaction to ensure data is persisted to disk
    conn.commit()
    print("NPS job completed successfully.")