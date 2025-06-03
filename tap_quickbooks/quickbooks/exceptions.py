# pylint: disable=super-init-not-called
class TapQuickbooksException(Exception):
    pass

class TapQuickbooksQuotaExceededException(TapQuickbooksException):
    pass

def raise_for_invalid_credentials(resp):
    def message_to_dict(input_string):
        # Split the string by commas to separate each key-value pair
        pairs = input_string.split("; ")
        
        # Create a dictionary by splitting each pair by '=' and using them as key-value
        result_dict = {key: value for key, value in (pair.split("=") for pair in pairs)}
        
        return result_dict

    try:
        response_dict_message = resp.json()["fault"]["error"][0]["message"]
        response_dict = message_to_dict(response_dict_message)
        if response_dict["statusCode"] == "403":
            raise TapQuickbooksException(f"[{response_dict['statusCode']}] Your credentials are invalid. Please check if you are using sandbox credentials to access production data and try again.")
    except TapQuickbooksException as ex:
        raise ex
    except:
        # Silently fail if our parsing is bad
        pass
