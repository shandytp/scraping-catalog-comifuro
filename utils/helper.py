import pandas as pd

def get_current_time():
    """
    Function yang digunakan untuk mendapatkan waktu sekarang
    yang digunakan untuk timestamp ketika scrapiing data

    Returns
    -------
    formatted_timestamp (timestamp): Timestamp yang didapatkan pada saat function digunakan
    """
    # Get the current timestamp
    current_timestamp = pd.Timestamp.now()

    # Convert the Timestamp object to a formatted string
    formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    return formatted_timestamp