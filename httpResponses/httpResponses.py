class CustomHttpResponses(object):
    def __init__(self):
        pass

    def setup_responses(self, msg, is_error, response_code, request_type, data):
        dict = {
            "msg": msg,
            "type": request_type,
            "response_code": response_code,
            "is_error": is_error,
            "data": data,
        }

        return dict

    def responses(self, msg, code, request_type, data=None):
        is_error = False
        response_code = code
        request_type = request_type

        return self.setup_responses(msg, is_error, response_code, request_type, data)
