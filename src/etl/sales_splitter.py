from datetime import datetime


def split_event(event: dict) -> list[tuple[str, dict]]:
    required = ['sale_id', 'user_id', 'product_id']
    if not all(k in event for k in required):
        return [('invalid', {
            "sale_id": event.get('sale_id', 'N/A'),
            "user_id": event.get('user_id', 'N/A'),
            "product_id": event.get('product_id', 'N/A'),
            "quantity": event.get('quantity', 0),
            "unit_price": event.get('unit_price', 0),
            "total_amount": event.get('total_amount', 0),
            "event_timestamp": event.get('event_timestamp', 'N/A'),
            "error_reason": "missing_field"
        })]

    if event.get('quantity', 0) <= 0:
        return [('invalid', {**event, "error_reason": "quantity_zero"})]

    if event.get('unit_price', 0) <= 0:
        return [('invalid', {**event, "error_reason": "negative_price"})]

    ts = event.get('event_timestamp', '')
    try:
        dt = datetime.fromisoformat(ts)
    except Exception:
        return [('invalid', {**event, "error_reason": "corrupted_timestamp"})]

    total = event.get('total_amount', 0)
    if total < 50:
        value_band = "low"
    elif total < 200:
        value_band = "medium"
    else:
        value_band = "high"

    enriched = {
        **event,
        "date_id": dt.strftime("%Y-%m-%d"),
        "value_band": value_band,
        "sale_hour": dt.hour,
        "sale_minute": dt.minute,
        "day_of_week": dt.strftime("%A"),
    }

    return [('valid', enriched)]
