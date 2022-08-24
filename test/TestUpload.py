import requests
from datetime import datetime
url='http://172.18.74.41:9099/models/yansi/predict'

begin = datetime.now()
slice_file = open('slice.jpg', "rb")
test_response = requests.post(url, files = {"filename": slice_file})
end = datetime.now()
check_cost = end-begin
print("Check image cost {} ms".format(check_cost.microseconds/1000))
print(test_response.text)