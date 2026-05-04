# GIS Data Migration Project: Django API Server

## Project Status

### Completed Steps ✅
1.  **Environment Setup**: Virtual environment created and dependencies (`django`, `djangorestframework`) installed.
2.  **Project Initialization**: Django project `api_service` and app `locations` initialized.
3.  **Configuration**: `settings.py` updated with required apps.
4.  **Core Implementation**: 
    - API views implemented using raw SQL for GIS compatibility.
    - URL routing configured for project and app levels.
5.  **Database Initialized**: Initial Django migrations applied (currently using SQLite as a placeholder).
6.  **System Audit**: Verified local installation of **PostgreSQL 16** and **PostGIS**.

### Pending Steps ⏳
1.  **PostgreSQL Connection**: Update `DATABASES` in `settings.py` to connect to your local PostgreSQL/PostGIS instance.
2.  **GIS Data Integration**: Verify or create the `locations_table` and `locations_detail_view` in the PostgreSQL database.
3.  **Endpoint Validation**: Test API responses against the live GIS database.
4.  **Security**: Configure `.env` for database credentials to keep `settings.py` clean.

---

## 1. Local Setup

### Create and Activate Virtual Environment
```bash
python -m venv venv
# Windows:
.\venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
```

### Install Dependencies
```bash
pip install django djangorestframework psycopg[binary]
```

### Initialize Django Project
```bash
django-admin startproject api_service .
python manage.py startapp locations
```

## 2. Configuration

Add `rest_framework` and `locations` to `INSTALLED_APPS` in `api_service/settings.py`:

```python
INSTALLED_APPS = [
    ...
    'rest_framework',
    'locations',
]
```

## 3. Implementing Custom SQL Endpoints

Django allows running raw SQL using `django.db.connection`. This is ideal for complex joins or existing database views.

### View Implementation (`locations/views.py`)

```python
from django.db import connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

def dictfetchall(cursor):
    """Return all rows from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

class LocationList(APIView):
    """
    Returns basic location information using raw SQL.
    """
    def get(self, request):
        with connection.cursor() as cursor:
            # You can call a VIEW or a custom SELECT here
            cursor.execute("SELECT id, name, latitude, longitude FROM locations_table")
            data = dictfetchall(cursor)
        return Response(data)

class LocationDetail(APIView):
    """
    Returns detailed location information for a specific ID.
    """
    def get(self, request, pk):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * 
                FROM locations_detail_view 
                WHERE id = %s
            """, [pk])
            data = dictfetchall(cursor)
            
        if not data:
            return Response({"error": "Location not found"}, status=status.HTTP_404_NOT_FOUND)
            
        return Response(data[0])

class LocationSearch(APIView):
    """
    Searches locations by name or description.
    """
    def get(self, request):
        query = request.query_params.get('q', '')
        if not query:
            return Response({"error": "Query parameter 'q' is required"}, status=status.HTTP_400_BAD_REQUEST)

        with connection.cursor() as cursor:
            # Case-insensitive search using ILIKE (PostgreSQL) or LIKE
            search_pattern = f"%{query}%"
            cursor.execute("""
                SELECT id, name, description 
                FROM locations_table 
                WHERE name ILIKE %s OR description ILIKE %s
            """, [search_pattern, search_pattern])
            data = dictfetchall(cursor)
            
        return Response(data)
```

## 4. URL Routing

### App URLs (`locations/urls.py`)
```python
from django.urls import path
from .views import LocationList, LocationDetail, LocationSearch

urlpatterns = [
    path('locations/', LocationList.as_view(), name='location-list'),
    path('locations/<int:pk>/', LocationDetail.as_view(), name='location-detail'),
    path('locations/search/', LocationSearch.as_view(), name='location-search'),
]
```

### Project URLs (`api_service/urls.py`)
```python
from django.urls import path, include

urlpatterns = [
    path('api/', include('locations.urls')),
]
```

## 5. Running the Server

1. **Apply Migrations** (if using Django models):
   ```bash
   python manage.py migrate
   ```
2. **Start Development Server**:
   ```bash
   python manage.py runserver
   ```

## 6. API Endpoints

### List Locations
- **URL**: `GET /api/locations/`
- **Description**: Returns a list of all locations with basic info.
- **SQL Source**: `SELECT id, name, latitude, longitude FROM locations_table`

### Location Details
- **URL**: `GET /api/locations/<id>/`
- **Description**: Returns full details for a specific location.
- **SQL Source**: `SELECT * FROM locations_detail_view WHERE id = %s`

### Search Locations
- **URL**: `GET /api/locations/search/?q=<term>`
- **Description**: Searches locations by name or description.
- **SQL Source**: `SELECT id, name, description FROM locations_table WHERE name ILIKE %s OR description ILIKE %s`

## Note on Database Views
If you are using existing GIS database views, ensure your database user has `SELECT` permissions on those views. You can call them in `cursor.execute("SELECT * FROM your_view_name")` just like a standard table.
