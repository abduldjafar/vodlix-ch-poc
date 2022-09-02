import requests
import json




def requests_api(config_file):
    with open(config_file, 'r') as config:
        test_schemas = json.load(config)

    print("=================================")
    for test_schema in test_schemas:
        print("request to endpoint {}".format(test_schema["endpoint"]))
        print("title action: {}".format(test_schema["title"]))
        if test_schema["method"] == "post":
            headers = test_schema["headers"]
            json_data = test_schema["data"]

            full_url = "{}{}".format(test_schema["base_url"], test_schema["endpoint"])
            response = requests.post(full_url, headers=headers, json=json_data)

        elif test_schema["method"] == "get":
            full_url = "{}{}".format(test_schema["base_url"], test_schema["endpoint"])
            headers = test_schema["headers"]
            response = requests.get(full_url, headers=headers)
        
        print("status responses: {}".format(response.status_code))
        print("message responses: {}".format(response.text))
        print("=================================")


if __name__ == "__main__":
    requests_api("test_endpoint_schema.json")