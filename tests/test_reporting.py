import pytest
from unittest.mock import Mock
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.reporting import generate_report
from app.models import MilkProduction, Weight, Cow

@pytest.fixture
def mock_db_session():
    return Mock(spec=Session)

def test_generate_report(mock_db_session):
    report_date = datetime(2024, 10, 14)

    mock_milk_data = [
        ('cow1', 15.0),
        ('cow2', 3.0),
    ]
    mock_db_session.query().filter().group_by().all.return_value = mock_milk_data

    mock_cows = [
        ('cow1', 'Bessie'),
        ('cow2', 'Molly'),
    ]
    mock_db_session.query(Cow.id, Cow.name).all.return_value = mock_cows

    def mock_latest_weight(cow_id):
        if cow_id == 'cow1':
            return (450.0,)
        elif cow_id == 'cow2':
            return (350.0,)
        return None

    mock_db_session.query(Weight.value).filter().order_by().first.side_effect = lambda *args, **kwargs: mock_latest_weight(args[1].__eq__.right.value)

    def mock_avg_weight(cow_id):
        if cow_id == 'cow1':
            return 455.0
        elif cow_id == 'cow2':
            return 390.0
        return None

    mock_db_session.query(func.avg(Weight.value)).filter().scalar.side_effect = lambda: mock_avg_weight(mock_db_session.query().filter.call_args[0][0].right.value)

    report = generate_report(mock_db_session, report_date)

    assert "cow1" in report
    assert "Total Milk Production: 15.0 liters" in report
    assert "Latest Weight: 450.0 kg" in report
    assert "30-day Avg Weight: 455.0 kg" in report

    assert "cow2" in report
    assert "Total Milk Production: 3.0 liters" in report
    assert "Latest Weight: 350.0 kg" in report
    assert "30-day Avg Weight: 390.0 kg" in report

    assert "Potentially Ill Cows:" in report
    assert "cow2" in report
