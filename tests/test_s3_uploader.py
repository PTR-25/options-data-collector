import os
import pytest
from unittest.mock import MagicMock, patch
from s3_uploader import S3Uploader

@pytest.fixture
def mock_s3():
    with patch('boto3.client') as mock_boto:
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        yield mock_client

@pytest.fixture
def uploader():
    return S3Uploader(max_workers=2, max_retries=2)

def test_upload_single_file(mock_s3, uploader, tmp_path):
    # Create test file
    test_file = tmp_path / "test.parquet"
    test_file.write_text("test data")
    
    success, uploaded = uploader.upload_to_s3(
        str(test_file),
        "test-bucket",
        "test-prefix"
    )
    
    assert success
    assert len(uploaded) == 1
    mock_s3.upload_file.assert_called_once()

def test_upload_directory(mock_s3, uploader, tmp_path):
    # Create test directory structure
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for i in range(3):
        test_file = data_dir / f"test_{i}.parquet"
        test_file.write_text(f"test data {i}")
    
    success, uploaded = uploader.upload_to_s3(
        str(data_dir),
        "test-bucket",
        "test-prefix"
    )
    
    assert success
    assert len(uploaded) == 3
    assert mock_s3.upload_file.call_count == 3

def test_upload_failure(mock_s3, uploader, tmp_path):
    test_file = tmp_path / "test.parquet"
    test_file.write_text("test data")
    
    mock_s3.upload_file.side_effect = Exception("Upload failed")
    
    success, uploaded = uploader.upload_to_s3(
        str(test_file),
        "test-bucket",
        "test-prefix"
    )
    
    assert not success
    assert len(uploaded) == 0
