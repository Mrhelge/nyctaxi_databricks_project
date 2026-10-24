from src.helper_utils.functions import months_to_load

def test_months_to_load():

    assert months_to_load(end_date="2026-10-31", lag_months=3, start_date="2026-05-01") == ["2026-05", "2026-06", "2026-07"]
    assert months_to_load(end_date="2026-02-15", lag_months=3, start_date="2025-10-01") == ["2025-10", "2025-11"]
    assert months_to_load(end_date="2025-07-20", lag_months=3, start_date="2025-04-01") == ["2025-04"]
    assert months_to_load(end_date="2025-05-10", lag_months=3, start_date="2025-04-01") == []
    



