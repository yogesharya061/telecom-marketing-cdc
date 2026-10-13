"""Final send-time rules. Flink selects candidates; this prevents stale sends."""
CAMPAIGN = 'DATA_TOPUP_V1'

def current_reason(row, billing_cycle):
    if row is None:
        return 'DELETED'
    if row['billing_cycle'] != billing_cycle:
        return 'STALE_CYCLE'
    if not row['marketing_consent']:
        return 'NO_CONSENT'
    if row['account_status'] != 'ACTIVE':
        return 'INACTIVE'
    if row['used_mb'] * 5 < row['quota_mb'] * 4:
        return 'BELOW_THRESHOLD'
    return 'ELIGIBLE'

def validate(event):
    if not isinstance(event, dict):
        raise ValueError('Expected JSON object')
    for field in ('event_id', 'customer_id', 'billing_cycle', 'campaign_id', 'reason'):
        if not isinstance(event.get(field), str) or not event[field]:
            raise ValueError(f'Missing/invalid {field}')
    if type(event.get('revision')) is not int or event['revision'] < 1:
        raise ValueError('Invalid revision')
    if event['campaign_id'] != CAMPAIGN:
        raise ValueError('Unknown campaign')
    if event['reason'] not in {'ELIGIBLE','NO_CONSENT','INACTIVE','BELOW_THRESHOLD','DELETED'}:
        raise ValueError('Invalid decision')
    return event
