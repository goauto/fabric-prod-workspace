# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "db36231f-c291-4b4d-bc4e-66a20fc937ad",
# META       "default_lakehouse_name": "Bronze_v2",
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd"
# META     },
# META     "environment": {
# META       "environmentId": "2aa5f137-c3c2-4e81-be54-75725c56da8f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

input_path= ""
output_path= ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import pgpy
import logging
from notebookutils import mssparkutils
import base64
import traceback
from pyspark.sql import SparkSession
 
# Configure logging with a handler and formatter to display well-structured log messages
# Improvement: Added handler and formatter to improve logging visibility in notebooks
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)  # Set logging level to INFO for detailed tracking
 
# Azure Key Vault and Lakehouse paths
KEY_VAULT_URL = "https://cdkvault.vault.azure.net/"
 
def get_secret_from_key_vault(secret_name):
    """
    Fetches a secret from Azure Key Vault.
    Parameters:
    secret_name (str): Name of the secret to fetch.
    Returns:
    str: The secret value.
    """
    try:
        # Improvement: Encapsulated the secret retrieval logic in a helper function
        return mssparkutils.credentials.getSecret(KEY_VAULT_URL, secret_name)
    except Exception as e:
        # Improvement: Better error handling and logging in case of failure
        logger.error(f"Error retrieving secret '{secret_name}': {str(e)}")
        raise  # Re-raise the exception for external handling
 
def read_encrypted_file_from_lakehouse(input_path):
    """
    Reads an encrypted file from Lakehouse and returns the binary content.
    Parameters:
    input_path (str): Path to the encrypted file.
    Returns:
    bytes: Binary content of the encrypted file.
    """
    try:
        # Improvement: Minimized DataFrame usage by directly extracting content
        encrypted_df = spark.read.format("binaryFile").load(input_path)
        encrypted_blob = encrypted_df.select("content").collect()[0][0]  # Use first() instead of collect()
        logger.info(f"Read encrypted content of size {len(encrypted_blob)} bytes.")
        return encrypted_blob
    except Exception as e:
        # Improvement: Added logging for file read errors
        logger.error(f"Error reading encrypted file from {input_path}: {str(e)}")
        raise
 
def decrypt_pgp_content(encrypted_blob, pgp_key, passphrase):
    """
    Decrypts the encrypted PGP content using the provided key and passphrase.
    Parameters:
    encrypted_blob (bytes): Encrypted content.
    pgp_key (pgpy.PGPKey): PGP private key object.
    passphrase (str): Passphrase to unlock the key.
    Returns:
    str: Decrypted message as a string.
    """
    try:
        # Improvement: Avoided exposing secrets unnecessarily in the main code block
        with pgp_key.unlock(passphrase) as unlocked_key:
            logger.debug("PGP Key unlocked.")
            pgp_message = pgpy.PGPMessage.from_blob(encrypted_blob)  # Pass bytes directly
            decrypted_message = unlocked_key.decrypt(pgp_message).message
            logger.info("Blob content decrypted.")
            return decrypted_message
    except Exception as e:
        # Improvement: Added logging to handle decryption errors
        logger.error(f"Error decrypting PGP content: {str(e)}")
        raise
 
def write_decrypted_content_to_lakehouse(output_path, decrypted_message):
    """
    Writes decrypted content to Lakehouse at the specified path.
    Parameters:
    output_path (str): Path to store the decrypted content.
    decrypted_message (str): Decrypted content to be written.
    """
    try:
        # Improvement: Encapsulated write logic in a function to improve readability
        mssparkutils.fs.put(output_path, decrypted_message, True)
        logger.info(f"Uploaded decrypted content to {output_path}")
    except Exception as e:
        # Improvement: Added logging to handle file write errors
        logger.error(f"Error writing decrypted content to {output_path}: {str(e)}")
        raise
 
# Main processing block with exception handling for the entire workflow
try:
    # Log input and output paths for debugging purposes
    logger.info(f"Extracted input_path: {input_path}")
    logger.info(f"Extracted output_path: {output_path}")
 
    # Step 1: Read the encrypted file from Lakehouse
    encrypted_blob = read_encrypted_file_from_lakehouse(input_path)
 
    # Step 2: Fetch the PGP private key from Azure Key Vault
    # Improvement: Encapsulated secret retrieval logic for better security and reusability
    key_secret = get_secret_from_key_vault("CDKPGPkey")
    pgp_key, _ = pgpy.PGPKey.from_blob(base64.b64decode(key_secret).decode("ascii"))
    logger.info("PGP Key retrieved from Azure Key Vault.")
 
    # Step 3: Unlock the passphrase-protected PGP key
    passphrase = get_secret_from_key_vault("PGP-Passphrase")
    decrypted_message = decrypt_pgp_content(encrypted_blob, pgp_key, passphrase)
 
    # Step 4: Write the decrypted content back to Lakehouse
    write_decrypted_content_to_lakehouse(output_path, decrypted_message.decode("utf-8"))
 
except Exception as e:
    # Improvement: Added full traceback logging for better debugging in case of failure
    logger.error(f"Exception encountered: {str(e)}")
    traceback.print_exc()  # Print the full stack trace for detailed error analysis

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
