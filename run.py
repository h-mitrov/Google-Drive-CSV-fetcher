"""
Main Google Doc csv downloading and parsing service module
"""
import argparse
import asyncio
import io
import json
import logging
from sys import platform

import aiohttp
from aiohttp import ClientError, ServerDisconnectedError, ClientConnectorError, ClientPayloadError
import pandas as pd

FILE_ID = "1zLdEcpzCp357s3Rse112Lch9EMUWzMLE"
ALLOWED_FIELDS = ("date", "campaign", "clicks")


class CSVReadingError(Exception):
    """
    Raises if we have csv reading problem
    """


class CSVValidationError(Exception):
    """
    Raises if we have csv validation problem
    """


class DownloadDataError(Exception):
    """
    Raises if we have problem with file download
    """


class FileProcessingService:
    """
    Downloads csv in public access from Google Docs,
    parses it to pandas dataframe and returns JSON object
    according to terminal arguments.
    """

    def __init__(self, fields: str | list[str], file_id: str):
        """
        :param fields: fields (among: date, campaign, clicks)
        :param file_id: google doc csv file id
        """
        self.file_id = file_id
        self.fields = fields

    def generate_fetch_url(self) -> str:
        """
        Fetches full download url using file id passed to the class
        :return: full download url
        """
        return 'https://drive.google.com/uc?id=' + self.file_id

    async def download_data(self) -> str:
        """
        Checks for download errors and returns csv download result as a stirng
        :return: csv file text as string
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.generate_fetch_url()) as response:
                    if response.status == 404:
                        raise DownloadDataError(f'Error while downloading data: {response.status}')
                    response.raise_for_status()
                    return await response.text()

        except (
                ClientError, ServerDisconnectedError, ClientConnectorError, ClientPayloadError
        ) as net_err:
            logging.error('Request error: %s', net_err)
            raise DownloadDataError(f'Error while downloading data: {net_err}') from net_err

    def validate_dataframe(self, csv_df: pd.DataFrame) -> None:
        """
        Check if dataframe contains necessary fields
        :param csv_df: dataframe obtained from Google Doc csv
        :return: None
        """
        missing_fields = [field for field in self.fields if field not in csv_df.columns]

        if missing_fields:
            raise CSVValidationError(
                f"The CSV is missing the following fields: {', '.join(missing_fields)}")

        logging.info("The CSV contains all the specified fields.")

    async def fetch_data(self) -> dict:
        """
        Get data from Google Doc csv, validate it and create pandas dataframe
        :return: pandas dataframe
        """
        try:
            downloaded_data = await self.download_data()
            csv_df = pd.read_csv(io.StringIO(downloaded_data))  # type: ignore
            self.validate_dataframe(csv_df)
            return csv_df[self.fields].to_dict(orient='records')
        except pd.errors.ParserError as parse_err:
            raise CSVReadingError(f"Error while reading CSV: {parse_err}") from parse_err

    async def process_google_csv(self) -> str:
        """
        Turn pandas dataframe to JSON object in specified format
        :return: JSON object
        """
        try:
            data = await self.fetch_data()
            return json.dumps({"data": data}, indent=4)
        except (CSVReadingError, CSVValidationError, DownloadDataError) as parse_err:
            return json.dumps({"error": str(parse_err)})


def validate_fields(value):
    """
    Command line arguments validation
    :param value: list of user-inputs
    :return: list of validated fields
    """
    field_list = value.split(',')
    for field in field_list:
        if field not in ALLOWED_FIELDS:
            raise argparse.ArgumentTypeError(
                f"'{field}' is not a valid field. Allowed fields are: {', '.join(ALLOWED_FIELDS)}")
    return field_list


async def main():
    """
    Main function that uses FileProcessingService method to get
    and parse csv file and prints the result
    :return: None
    """
    parser = argparse.ArgumentParser(description='Get data from a CSV file hosted on Google Drive')
    parser.add_argument(
        '--fields',
        required=True,
        type=validate_fields,
        help='Comma-separated fields (among these: date,campaign,clicks)'
    )
    args = parser.parse_args()
    file_processing_service = FileProcessingService(file_id=FILE_ID, fields=args.fields)
    print(await file_processing_service.process_google_csv())


if __name__ == "__main__":
    if platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
