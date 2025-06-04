def save_api_usage(method, url, params, body, response):
    import json
    import datetime

    request_dict = {
        "method": method,
        "url": url,
        "params": params,
        "body": body,
    }

    for key, value in request_dict.copy().items():
        if value is None:
            request_dict.pop(key)

    usage_data = {
        "timestamp": datetime.datetime.now().timestamp(),
        "request": request_dict,
        "response_status": response.status_code if response else None,
    }

    with open("api_usage.jsonl", "a") as f:
        f.write(json.dumps(usage_data, default=str) + "\n")
