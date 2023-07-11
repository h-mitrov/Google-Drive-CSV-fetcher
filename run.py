import asyncio
import io
import json
import logging
from sys import platform

import aiohttp
import argparse
import pandas as pd
from aiohttp import (
    ClientError,
    ServerDisconnectedError,
    ClientConnectorError,
    ClientPayloadError,
)

FILE_ID = "1zLdEcpzCp357s3Rse112Lch9EMUWzMLE"
ALLOWED_FIELDS = ("date", "campaign", "clicks", "spend", "medium", "source")


class CSVReadingError(Exception):
    pass


class CSVValidationError(Exception):
    pass


class DownloadDataError(Exception):
    pass


class FileProcessingService:
    def __init__(self, fields: str | list[str], file_id: str):
        self.file_id = file_id
        self.fields = fields

    def generate_fetch_url(self) -> str:
        return "https://drive.google.com/uc?id=" + self.file_id

    async def download_data(self) -> str:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.generate_fetch_url()) as response:
                    if response.status == 404:
                        raise DownloadDataError(
                            f"Error while downloading data: {response.status}"
                        )
                    response.raise_for_status()
                    return await response.text()

        except (
            ClientError,
            ServerDisconnectedError,
            ClientConnectorError,
            ClientPayloadError,
        ) as e:
            logging.error(f"Request error: {e}")
            raise DownloadDataError(f"Error while downloading data: {e}")

    def validate_dataframe(self, df: pd.DataFrame) -> None:
        missing_fields = [field for field in self.fields if field not in df.columns]

        if missing_fields:
            raise CSVValidationError(
                f"The CSV is missing the following fields: {', '.join(missing_fields)}"
            )

        logging.info("The CSV contains all the specified fields.")

    async def fetch_data(self) -> dict:
        try:
            downloaded_data = await self.download_data()
            df = pd.read_csv(io.StringIO(downloaded_data))  # type: ignore
            self.validate_dataframe(df)
            return df[self.fields].to_dict(orient="records")
        except pd.errors.ParserError as e:
            raise CSVReadingError(f"Error while reading CSV: {e}")

    async def process_google_csv(self) -> str:
        try:
            data = await self.fetch_data()
            return json.dumps({"data": data}, indent=4)
        except (CSVReadingError, CSVValidationError, DownloadDataError) as e:
            return json.dumps({"error": str(e)})


def validate_fields(value):
    field_list = value.split(",")
    for field in field_list:
        if field not in ALLOWED_FIELDS:
            raise argparse.ArgumentTypeError(
                f"'{field}' is not a valid field. Allowed fields are: {', '.join(ALLOWED_FIELDS)}"
            )
    return field_list


async def main():
    parser = argparse.ArgumentParser(
        description="Get data from a CSV file hosted on Google Drive"
    )
    parser.add_argument(
        "--fields",
        required=True,
        type=validate_fields,
        help="Comma-separated fields (among these: date,campaign,clicks)",
    )
    args = parser.parse_args()
    file_processing_service = FileProcessingService(file_id=FILE_ID, fields=args.fields)
    print(await file_processing_service.process_google_csv())


if __name__ == "__main__":
    if platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
