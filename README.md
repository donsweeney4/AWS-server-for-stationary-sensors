StationaryNetworkServer/

This branch removes the Lifetime option and replaces it with a custom date range and date start


│
├── app.py
├── config.py
├── database.py
├── routes/
│   ├── __init__.py
│   ├── map_data.py
│   └── index.py
├── static/
│   ├── styles.css
│   └── script.js
├── templates/
│   ├── index.html
└── utils/
    ├── __init__.py
    ├── weather_data.py
    └── plot_generation.py


app.py: Main application file that initializes the app and 
registers routes.

config.py: Stores configuration settings, which makes it easy 
to manage and change credentials or API keys without affecting 
the main logic.

database.py: Encapsulates database access logic, making it easier 
to manage database interactions and ensure connections are 
properly opened and closed.

routes/: Organizes different endpoints logically, allowing for 
easier navigation and modification of routes.

utils/: Contains utility functions that are reused across the 
application, reducing code duplication.

templates/ and static/: Keep the HTML, CSS, and JS separate 
from Python code, following best practices for web development.