# Fantasy Football Predictions Project

## Overview

This project sets up BigQuery tables for storing information about football teams, players, games, and stadiums. The database schema includes four core tables: `Players`, `Teams`, `Games`, and `Stadiums`.

---

## Getting Started

### Prerequisites

Before getting started, ensure you have the following:

- **Google Cloud project** set up.
- **BigQuery API** enabled in your Google Cloud project.
- A **service account** with the necessary BigQuery permissions.
- **Python 3.9+** installed on your local machine.

### Step 1: Set up the Virtual Environment

1. **Create a virtual environment** (recommended): 

   In the terminal or command prompt, navigate to your project directory and run:

   ```bash
   python -m venv venv

2. **Activate Virtual Environment**
    ```bash
    venv\Scripts\activate

3. **Gcloud Authentication**'
    Set up Google Cloud SDK:
    If you haven't installed the Google Cloud SDK yet, follow the instructions here: https://cloud.google.com/sdk/docs/install
    Authenticate with your Google Cloud account:

    Run the following command in the terminal to log in:
    ```bash
    gcloud auth login

    gcloud config set project PROJECT_ID
    Replace PROJECT_ID with your actual Google Cloud project ID in config settings.


To create the dataset and tables you could run this command but the tables have already been created.  The next step is to update these tables with data from various api's
python src/db/setup_bigquery.py



