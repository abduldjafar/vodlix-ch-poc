class CustomHttpResponses(object):
    def __init__(self):
        pass

    def setup_responses(self,msg, is_error, response_code, request_type, data):
        dict = {
            "msg": msg,
            "type": request_type,
            "response_code": response_code,
            "is_error": is_error,
            "data": data,
        }

        return dict


    def post_success_responses(self,msg, data=None):
        is_error = False
        response_code = 200
        request_type = "POST"

        return self.setup_responses(msg, is_error, response_code, request_type, data)
    
    def post_failed_responses(self,msg, data=None):
        is_error = True
        response_code = 500
        request_type = "POST"

        return self.setup_responses(msg, is_error, response_code, request_type, data)


    def delete_success_responses(self,msg, data=None):
        is_error = False
        response_code = 200
        request_type = "DELETE"

        return self.setup_responses(msg, is_error, response_code, request_type, data)


    def get_success_responses(self,msg, data=None):
        is_error = False
        response_code = 200
        request_type = "GET"

        return self.setup_responses(msg, is_error, response_code, request_type, data)


    def put_success_responses(self,msg, data=None):
        is_error = False
        response_code = 200
        request_type = "PUT"

        return self.setup_responses(msg, is_error, response_code, request_type, data)