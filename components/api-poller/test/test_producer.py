from unittest.mock import patch, MagicMock
from src.producer.app import lambda_handler

@patch("src.producer.app.config", {"regions": [{"region_id": "1234"}]})
@patch("src.producer.app.boto3.client")
def test_lambda_handler(mock_boto_client, monkeypatch):
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs

    monkeypatch.setenv("QUEUE_URL", "test-queue-url")
    result = lambda_handler({}, None)

    mock_sqs.send_message.assert_called_once_with(
        QueueUrl="test-queue-url",
        MessageBody='{"region_id": "1234"}'
    )
    assert result["statusCode"] == 200
