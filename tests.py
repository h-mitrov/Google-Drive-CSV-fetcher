import pytest
from run import FileProcessingService, DownloadDataError, CSVValidationError
import json


@pytest.fixture(scope='session')
def file_id() -> str:
    return '1zLdEcpzCp357s3Rse112Lch9EMUWzMLE'


async def test_download_data(file_id):
    file_processing_service = FileProcessingService(file_id=file_id, fields=[])
    data = await file_processing_service.download_data()
    assert isinstance(data, str)
    assert len(data) > 0


async def test_download_data_invalid_url(file_id, caplog):
    file_processing_service = FileProcessingService(file_id=f"{file_id}/new/error", fields=[])

    with pytest.raises(DownloadDataError) as exc_info:
        await file_processing_service.download_data()

    assert str(exc_info.value) == "Error while downloading data: 404"

    for record in caplog.records:
        assert "Error while downloading data:" in record.message


@pytest.mark.parametrize('fields', [
    pytest.param(['date'], id='Date'),
    pytest.param(['date', 'campaign'], id='Date, campaign'),
    pytest.param(['date', 'campaign', 'clicks'], id='Date, campaign, clicks')
])
async def test_fetch_data(file_id, fields, monkeypatch):
    file_processing_service = FileProcessingService(file_id=file_id, fields=fields)

    async def mock_results():
        with open('test_task_data.csv') as file:
            text = file.read()
        return text

    monkeypatch.setattr(file_processing_service, 'download_data', mock_results)

    data = await file_processing_service.fetch_data()
    assert isinstance(data, list)
    assert len(data) > 0


async def test_fetch_data_invalid_field(file_id, monkeypatch, caplog):
    fields = ['dat']
    file_processing_service = FileProcessingService(file_id=file_id, fields=fields)

    async def mock_results():
        with open('test_task_data.csv') as file:
            text = file.read()
        return text

    monkeypatch.setattr(file_processing_service, 'download_data', mock_results)

    with pytest.raises(CSVValidationError) as exc_info:
        await file_processing_service.fetch_data()

    assert str(exc_info.value) == "The CSV is missing the following fields: dat"

    for record in caplog.records:
        assert "The CSV is missing the following fields:" in record.message


async def test_process_google_csv(file_id, monkeypatch):
    fields = ['date', 'campaign']
    file_processing_service = FileProcessingService(file_id=file_id, fields=fields)

    async def mock_results():
        with open('test_task_data.csv') as file:
            text = file.read()
        return text

    monkeypatch.setattr(file_processing_service, 'download_data', mock_results)

    result = await file_processing_service.process_google_csv()
    assert isinstance(result, str)
    assert len(result) > 0
    assert 'data' in result


async def test_keys_in_csv_data(file_id, monkeypatch):
    fields = ['date', 'campaign', 'clicks']
    file_processing_service = FileProcessingService(file_id=file_id, fields=fields)

    async def mock_results():
        with open('test_task_data.csv') as file:
            text = file.read()
        return text

    monkeypatch.setattr(file_processing_service, 'download_data', mock_results)

    result = await file_processing_service.process_google_csv()
    response = json.loads(result)
    data = response['data']
    assert isinstance(data, list)
    assert len(data) > 0
    first_obj = data[0]
    assert 'date' in first_obj
    assert 'campaign' in first_obj
    assert 'clicks' in first_obj


if __name__ == '__main__':
    pytest.main()
