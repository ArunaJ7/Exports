------------------------------------------------------------------------
Python Project File Structure
------------------------------------------------------------------------
DRS_Python_Backend/
        ├── openApi/
        │      ├── routes                               # Define API routes
        │      ├── controllers                          # Define API endpoints 
        │      ├── models                               # Define data models for the API 
        │      ├── services                             # API logic
        │
        ├── logger/
        │      ├── log_config.py                        # Logger configuration
        │      ├── log_manager.py                       # Utility functions for logging
        │
        ├── process_logic/
               ├──
        │
        ├── tests/
        │      ├── test_api.py                          # Test cases for API endpoints
        │      ├── test_database.py                     # Test cases for database interactions
        │      ├── test_logger.py                       # Test cases for logger
        │      ├── test_process_logic.py                # Test cases for process logic
        │
        ├── utils/
        │      ├── database
        │      │        ├── db_config.py                # Database connection setup
        │      │        ├── migrations/                 # For database migrations
        │      │              └── (migration scripts)
        │      │
        │      ├── email
        │
        ├── main.py                                     # Entry point of the application
        ├── requirements.txt                            # Dependencies
        ├── README.md                                   # Project documentation
        ├── .env                                        # Environment variables (gitignored)
        ├── .gitignore                                  # Files/folders to ignore in version control
