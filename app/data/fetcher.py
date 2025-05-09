from app.utils.logger import setup_logger
logger = setup_logger()

def get_crypto_data():
    logger.info("Fetching crypto data started")
    # fetch data...
    logger.info("Crypto data fetched successfully")
