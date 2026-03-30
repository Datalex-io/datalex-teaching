import json

# --- CONFIGURATION ---
SECRET_TOKEN = "xxXXXxxx"

def lambda_handler(event, context):
    # 1. Vérification de la sécurité (Header x-esme-token)
    headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
    if headers.get('x-esme-token') != SECRET_TOKEN:
        return {'statusCode': 403, 'body': json.dumps({'error': 'Unauthorized'})}

    # 2. Récupération des données Tally
    try:
        body = json.loads(event.get('body', '{}'))
        fields = body.get('data', {}).get('fields', [])
    except Exception:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid JSON'})}

    result = {}

    # Helper pour extraire le texte des menus déroulants (Dropdowns)
    def get_text(field):
        val_ids = field.get('value', [])
        if not val_ids or not isinstance(val_ids, list): return None
        target_id = val_ids[0]
        options = field.get('options', [])
        return next((opt['text'] for opt in options if opt['id'] == target_id), None)

    # 3. Mapping intelligent par Label
    for f in fields:
        label = f.get('label', '')
        val = f.get('value')

        if label == "first_name": 
            result['first_name'] = val
        elif label == "last_name": 
            result['last_name'] = val
        elif label == "email_addresses":
            result['email'] = val
            if val and "@" in val:
                result['domain'] = val.split('@')[-1]
        elif label == "Company name": 
            result['company_name'] = val
        elif label == "kind_of_work": 
            result['job_function'] = get_text(f)
        elif label == "What is your role?": 
            result['job_role'] = get_text(f)
        elif label == "Licence Type": 
            text = get_text(f)
            result['license_type'] = text
            result['is_beta'] = (text == "BETA")
        elif label == "lead_gen": 
            result['lead_source'] = get_text(f)

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(result)
    }
    headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
    if headers.get('x-esme-token') != SECRET_TOKEN:
        return {'statusCode': 403, 'body': json.dumps({'error': 'Unauthorized'})}

    # 2. Récupération des données
    try:
        # La Function URL envoie le body sous forme de string
        body = json.loads(event.get('body', '{}'))
        fields = body.get('data', {}).get('fields', [])
    except Exception:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid JSON'})}

    result = {}

    # Helper pour extraire le texte des menus déroulants (Dropdowns)
    def get_text(field):
        val_ids = field.get('value', [])
        if not val_ids: return None
        target_id = val_ids[0]
        return next((opt['text'] for opt in field.get('options', []) if opt['id'] == target_id), None)

    # 3. Mapping intelligent par Label (plus sûr que les index)
    for f in fields:
        label = f.get('label', '')
        val = f.get('value')

        if label == "first_name": result['first_name'] = val
        elif label == "last_name": result['last_name'] = val
        elif label == "email_addresses":
            result['email'] = val
            if val and "@" in val:
                result['domain'] = val.split('@')[-1]
        elif label == "Company name": result['company_name'] = val
        elif label == "kind_of_work": result['job_function'] = get_text(f)
        elif label == "What is your role?": result['job_role'] = get_text(f)
        elif label == "Licence Type": 
            result['license_type'] = get_text(f)
            result['is_beta'] = (get_text(f) == "BETA")
        elif label == "lead_gen": result['lead_source'] = get_text(f)

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(result)
    }