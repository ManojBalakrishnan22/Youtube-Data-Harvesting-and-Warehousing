# YouTube Data Harvesting and Warehousing

## Introduction

This project involves creating a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application enables users to input a YouTube channel ID, retrieve relevant data using the Google YouTube Data API v3, and store this data into a SQL database. Users can then query the data warehouse with SQL and display the results in the Streamlit app.

## Technologies Used

- **Python**: For scripting and interacting with APIs.
- **API Integration**: Google YouTube Data API v3 to fetch YouTube channel and video data.
- **Data Collection**: Collecting video and channel data from YouTube.
- **Streamlit**: For creating the interactive web application.
- **MySQL**: For storing and querying the data.

## Setup Instructions

### Prerequisites

1. **Google API Key**: Obtain an API key from the [Google Cloud Console](https://console.cloud.google.com/).
2. **MySQL Server**: Ensure MySQL server is installed and running.

### Installation

1. **Clone the Repository**

    ```bash
    cd your-directory
    git clone https://github.com/ManojBalakrishnan22/Youtube-Data-Harvesting-and-Warehousing.git
    ```

2. **Install Required Python Packages**

    ```bash
    pip install google-api-python-client mysql-connector-python pandas streamlit
    ```


3. **Configure API Key**

    - Open `youtube.py` and replace `'YOUR_YOUTUBE_API_KEY'` with your actual API key.

### Running the Application

1. **Start the Streamlit Application**

    ```bash
    streamlit run app.py
    ```

2. **Access the Application**

    - Open your web browser and navigate to `http://localhost:8501`.
### Screen Shots
Data Collection
![data collection](https://github.com/user-attachments/assets/b987ad8e-6664-47ee-92cb-17f70604a196)

Data Analysis
![data_analysis](https://github.com/user-attachments/assets/0f4ec001-6182-40bb-94a2-9e90e4cce66f)


## Usage

1. **Enter YouTube Channel ID**: Type in the YouTube channel ID in the input field.
2. **Fetch Data**: Click the "Get Channel Details" button to retrieve and store channel and video data.
3. **View Data**: The application will display:
   - Channel information
   - Data stored in the MySQL database
   - A DataFrame view of the stored channel data


## Contributing

Feel free to fork this repository, make improvements, and submit pull requests. Issues and feature requests can be reported via GitHub Issues.


## Contact

For any questions or feedback, please reach out to: [Manoj .B](mailto:bmanoj1122000@gmail.com)

